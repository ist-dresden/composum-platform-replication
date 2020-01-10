package com.composum.platform.replication.remote;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.platform.commons.logging.Message;
import com.composum.platform.commons.logging.MessageContainer;
import com.composum.platform.replication.json.ChildrenOrderInfo;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.platform.replication.remotereceiver.RemoteReceiverConstants;
import com.composum.platform.replication.remotereceiver.UpdateInfo;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.platform.staging.ReleaseChangeEventListener;
import com.composum.sling.platform.staging.ReleaseChangeProcess;
import com.composum.sling.platform.staging.StagingReleaseManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.NonExistingResource;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.composum.sling.core.util.SlingResourceUtil.isSameOrDescendant;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.awaiting;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.error;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.success;
import static java.util.Objects.requireNonNull;

/**
 * Transmits the changes of the JCR content of a release to a remote system.
 * We transmit the subtrees of all resources changed in the event as a zip to the
 * {@link RemotePublicationReceiverServlet}.
 */
@Component(
        service = ReleaseChangeEventListener.class,
        property = {Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Publisher Service"},
        immediate = true)
@Designate(ocd = RemotePublisherService.Configuration.class)
public class RemotePublisherService implements ReleaseChangeEventListener {

    protected static final Logger LOG = LoggerFactory.getLogger(RemotePublisherService.class);

    public static final String PATH_CONFIGROOT = "/conf";
    public static final String DIR_REPLICATION = "/replication";

    protected volatile Configuration config;

    protected volatile CloseableHttpClient httpClient;

    @Reference
    protected NodesConfiguration nodesConfig;

    @Reference
    protected ThreadPoolManager threadPoolManager;

    @Reference
    protected StagingReleaseManager releaseManager;

    @Reference
    protected CryptoService cryptoService;

    @Reference
    protected ResourceResolverFactory resolverFactory;

    protected final Map<String, RemoteReleasePublishingProcess> processesCache = Collections.synchronizedMap(new HashMap<>());

    @Nullable
    @Override
    public Collection<RemoteReleasePublishingProcess> processesFor(@Nullable StagingReleaseManager.Release release) {
        if (release == null || !isEnabled()) { return Collections.emptyList(); }

        Collection<RemoteReleasePublishingProcess> result =
                processesFor(release.getReleaseRoot()).stream()
                        .filter(process -> process.appliesTo(release))
                        .collect(Collectors.toList());
        return result;
    }

    @Nonnull
    @Override
    public Collection<RemoteReleasePublishingProcess> processesFor(@Nullable Resource releaseRoot) {
        if (releaseRoot == null || !isEnabled()) { return Collections.emptyList(); }

        ResourceResolver resolver = releaseRoot.getResourceResolver();
        BeanContext context = new BeanContext.Service(resolver);

        List<RemotePublicationConfig> replicationConfigs = getReplicationConfigs(releaseRoot, context);
        Collection<RemoteReleasePublishingProcess> processes = new ArrayList<>();
        for (RemotePublicationConfig replicationConfig : replicationConfigs) {
            if (!replicationConfig.isEnabled()) {
                processesCache.remove(replicationConfig.getPath());
                continue;
            }
            RemoteReleasePublishingProcess process = processesCache.computeIfAbsent(replicationConfig.getPath(),
                    (k) -> new RemoteReleasePublishingProcess(releaseRoot, replicationConfig)
            );
            processes.add(process);
        }
        return processes;
    }

    /** Creates the service resolver used to update the content. */
    @Nonnull
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    @Nonnull
    protected List<RemotePublicationConfig> getReplicationConfigs(@Nonnull Resource releaseRoot,
                                                                  @Nonnull BeanContext context) {
        String releasePath = releaseRoot.getPath();
        String configparent = PATH_CONFIGROOT + releasePath + DIR_REPLICATION;

        List<RemotePublicationConfig> configs = new ArrayList<>();
        Resource configroot = releaseRoot.getResourceResolver().getResource(configparent);
        if (configroot != null) {
            for (Resource child : configroot.getChildren()) {
                RemotePublicationConfig replicationConfig =
                        context.withResource(child).adaptTo(RemotePublicationConfig.class);
                if (replicationConfig != null) { configs.add(replicationConfig); }
            }
        }
        return configs;
    }

    @Activate
    @Modified
    protected void activate(final Configuration theConfig) {
        LOG.info("activated");
        this.config = theConfig;
        this.httpClient = HttpClients.createDefault();
    }

    @Deactivate
    protected void deactivate() throws IOException {
        LOG.info("deactivated");
        processesCache.clear();
        try (CloseableHttpClient ignored = httpClient) { // just make sure it's closed.
            this.config = null;
            this.httpClient = null;
        }
    }

    protected boolean isEnabled() {
        RemotePublisherService.Configuration theconfig = this.config;
        boolean enabled = theconfig != null && theconfig.enabled();
        if (!enabled) { processesCache.clear(); }
        return enabled;
    }

    protected class RemoteReleasePublishingProcess implements ReleaseChangeProcess {
        // we deliberately save nothing that refers to resolvers, since this is an object that lives long
        @Nonnull
        protected final String configPath;
        @Nonnull
        protected final String releaseRootPath;
        protected final String name;
        protected final String description;
        protected volatile MessageContainer messages = new MessageContainer();
        protected final Object changedPathsChangeLock = new Object();
        @Nonnull
        protected volatile Set<String> changedPaths = new LinkedHashSet<>();
        protected volatile String releaseUuid;
        protected volatile Date finished;
        protected volatile ReleaseChangeProcessorState state;
        protected volatile Date startedAt;
        protected volatile ReplicatorStrategy runningStrategy;
        protected volatile Thread runningThread;

        public RemoteReleasePublishingProcess(Resource releaseRoot, RemotePublicationConfig config) {
            configPath = config.getPath();
            releaseRootPath = releaseRoot.getPath();
            name = config.getName();
            description = config.getDescription();
        }


        public boolean appliesTo(StagingReleaseManager.Release release) {
            ResourceResolver resolver = release.getReleaseRoot().getResourceResolver();
            RemotePublicationConfig publicationConfig = new BeanContext.Service(resolver).adaptTo(RemotePublicationConfig.class);
            List<String> marks = release.getMarks();
            return publicationConfig != null && publicationConfig.isEnabled() && (
                    marks.contains(publicationConfig.getReleaseMark().toLowerCase())
                            || marks.contains(publicationConfig.getReleaseMark().toUpperCase()));
        }

        @Override
        public void triggerProcessing(@Nonnull ReleaseChangeEvent event) {
            if (!isEnabled()) { return; }
            if (!appliesTo(event.release())) { // shouldn't even be called.
                LOG.warn("Received event for irrelevant release {}", event);
                return;
            }
            LOG.info("adding event {}", event);

            boolean restart = !StringUtils.equals(releaseUuid, event.release().getUuid()) || state == error;
            releaseUuid = event.release().getUuid();

            synchronized (changedPathsChangeLock) {
                Set<String> newChangedPaths = new LinkedHashSet<>(changedPaths);
                newChangedPaths.addAll(event.newOrMovedResources());
                newChangedPaths.addAll(event.removedOrMovedResources());
                newChangedPaths.addAll(event.updatedResources());
                newChangedPaths = cleanupPaths(newChangedPaths);
                if (!newChangedPaths.equals(changedPaths)) {
                    restart = true;
                    changedPaths = newChangedPaths;
                }
            }

            if (restart) {
                if (runningStrategy != null) { runningStrategy.setAbortAtNextPossibility(); }
                state = ReleaseChangeProcessorState.awaiting;
                startedAt = null;
                finished = null;
                messages = new MessageContainer(LOG);
            }
        }

        /** Removes paths that are contained in other paths. */
        @Nonnull
        protected Set<String> cleanupPaths(@Nonnull Iterable<String> paths) {
            Set<String> cleanedPaths = new LinkedHashSet<>();
            for (String path : paths) {
                if (cleanedPaths.stream().anyMatch((p) -> isSameOrDescendant(p, path))) { continue; }
                cleanedPaths.removeIf((p) -> isSameOrDescendant(path, p));
                cleanedPaths.add(path);
            }
            return cleanedPaths;
        }

        @Override
        public void run() {
            if (state != ReleaseChangeProcessorState.awaiting || changedPaths.isEmpty() || !isEnabled()) {
                LOG.info("Nothing to do in {} state {}", name, state);
                return;
            }
            state = ReleaseChangeProcessorState.processing;
            startedAt = new Date();
            try (ResourceResolver serviceResolver = makeResolver()) { // FIXME(hps,07.01.20) more error checks

                LOG.info("Starting run of {}", name);
                Resource configResource = serviceResolver.getResource(configPath);
                RemotePublicationConfig replicationConfig = new BeanContext.Service(serviceResolver)
                        .withResource(configResource).adaptTo(RemotePublicationConfig.class);
                if (replicationConfig == null || !replicationConfig.isEnabled()) {
                    LOG.warn("Disabled / unreadable config, not run: {}", name);
                    return; // warning - should normally have been caught before
                }

                Resource releaseRoot = requireNonNull(serviceResolver.getResource(releaseRootPath), releaseRootPath);
                StagingReleaseManager.Release release = releaseManager.findReleaseByUuid(releaseRoot, releaseUuid);
                ResourceResolver releaseResolver = releaseManager.getResolverForRelease(release, null, false);
                BeanContext.Service context = new BeanContext.Service(releaseResolver);

                Set<String> processedChangedPaths = swapOutChangedPaths();
                try {
                    ReplicatorStrategy strategy = new ReplicatorStrategy(processedChangedPaths, release, context, replicationConfig);

                    if (runningStrategy != null) {
                        runningStrategy.setAbortAtNextPossibility();
                        Thread.sleep(5000);
                        if (runningThread != null) {
                            runningThread.interrupt();
                            Thread.sleep(2000);
                        }
                    }

                    runningStrategy = strategy;
                    startedAt = new Date();
                    try {
                        runningThread = Thread.currentThread();
                        runningStrategy.replicate();
                    } finally {
                        runningThread = null;
                    }
                    state = success;
                    processedChangedPaths.clear();
                } finally {
                    maybeResetChangedPaths(processedChangedPaths);
                }

            } catch (LoginException e) { // misconfiguration
                messages.add(Message.error("Can't get service resolver"), e);
            } catch (InterruptedException e) {
                LOG.error("Interrupted " + e, e);
                messages.add(Message.warn("Interrupted"), e);
            } catch (ReplicationFailedException | RuntimeException e) {
                messages.add(Message.error("Other error: ", e.toString()), e);
            } finally {
                if (state != success && state != awaiting) {
                    runningStrategy = null;
                    state = error;
                }
                finished = new Date();
                LOG.info("Finished run with {} : {} - @{}", state, name, System.identityHashCode(this));
            }
        }

        /** Returns the current paths in {@link #changedPaths} resetting {@link #changedPaths}. */
        @Nonnull
        protected Set<String> swapOutChangedPaths() {
            Set<String> processedChangedPaths;
            synchronized (changedPathsChangeLock) {
                processedChangedPaths = changedPaths;
                changedPaths = new LinkedHashSet<>();
            }
            return processedChangedPaths;
        }

        /** Adds unprocessed paths taken out of {@link #changedPaths} with {@link #swapOutChangedPaths()} back. */
        protected void maybeResetChangedPaths(Set<String> unProcessedChangedPaths) {
            if (!unProcessedChangedPaths.isEmpty()) { // add them back
                synchronized (changedPathsChangeLock) {
                    if (changedPaths.isEmpty()) {
                        changedPaths = unProcessedChangedPaths;
                    } else { // some events arrived in the meantime
                        unProcessedChangedPaths.addAll(changedPaths);
                        changedPaths = cleanupPaths(unProcessedChangedPaths);
                        if (!changedPaths.isEmpty()) { state = awaiting; }
                    }
                }
            }
        }

        @Override
        public int getCompletionPercentage() {
            switch (state) {
                case processing:
                    return runningStrategy != null ? runningStrategy.progress : 0;
                case success:
                case error:
                    return 100;
                case idle:
                case awaiting:
                default:
                    return 0;
            }
        }

        @Nonnull
        @Override
        public ReleaseChangeProcessorState getState() {
            return state;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Nullable
        @Override
        public Date getRunStartedAt() {
            return startedAt;
        }

        @Nullable
        @Override
        public Date getRunFinished() {
            return finished;
        }

        @Nonnull
        @Override
        public MessageContainer getMessages() {
            return messages;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("state", state)
                    .toString();
        }
    }

    /** Responsible for one replication. */
    protected class ReplicatorStrategy {
        @Nonnull
        protected final Set<String> changedPaths;
        @Nonnull
        protected final StagingReleaseManager.Release release;
        @Nonnull
        protected final ResourceResolver resolver;
        @Nonnull
        protected final BeanContext context;
        @Nonnull
        protected final RemotePublicationConfig replicationConfig;
        @Nonnull
        protected final String originalReleaseChangeNumber;
        @Nonnull
        protected final RemotePublicationReceiverFacade publisher;

        protected volatile int progress;

        /** If set, the replication process is aborted at the next step when this is checked. */
        protected volatile boolean abortAtNextPossibility = false;

        protected ReplicatorStrategy(@Nonnull Set<String> changedPaths, @Nonnull StagingReleaseManager.Release release,
                                     @Nonnull BeanContext context, @Nonnull RemotePublicationConfig replicationConfig) {
            this.changedPaths = changedPaths;
            this.release = release;
            this.originalReleaseChangeNumber = release.getChangeNumber();
            this.resolver = context.getResolver();
            this.context = context;
            this.replicationConfig = replicationConfig;

            publisher = new RemotePublicationReceiverFacade(replicationConfig,
                    context, httpClient, () -> config, cryptoService, nodesConfig);
        }

        /**
         * Sets a mark that leads to aborting the process at the next step - if an outside interruption is necessary
         * for some reason.
         */
        public void setAbortAtNextPossibility() {
            abortAtNextPossibility = true;
        }

        protected void replicate() throws ReplicationFailedException {
            UpdateInfo cleanupUpdateInfo = null;
            try {
                String commonParent = SlingResourceUtil.commonParent(changedPaths);
                LOG.info("Changed paths below {}: {}", commonParent, changedPaths);
                requireNonNull(commonParent);
                progress = 0;

                UpdateInfo updateInfo = publisher.startUpdate(release.getReleaseRoot().getPath(), commonParent);
                cleanupUpdateInfo = updateInfo;
                LOG.info("Received UpdateInfo {}", updateInfo);

                if (originalReleaseChangeNumber.equals(updateInfo.originalPublisherReleaseChangeId)) {
                    LOG.info("Abort publishing since content on remote system is up to date.");
                    abort(updateInfo);
                    return;
                }

                RemotePublicationReceiverServlet.ContentStateStatus contentState = publisher.contentState(updateInfo,
                        changedPaths, resolver, release.getReleaseRoot().getPath());
                if (!contentState.isValid()) {
                    LOG.error("Received invalid status on contentState for {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Querying content state failed for " + replicationConfig,
                            null, null);
                }
                LOG.info("Content difference on remote side: {} , deleted {}",
                        contentState.getVersionables().getChanged(), contentState.getVersionables().getDeleted());
                abortIfNecessary(updateInfo);

                Status compareContentState = publisher.compareContent(updateInfo, changedPaths, resolver, commonParent);
                if (!compareContentState.isValid()) {
                    LOG.error("Received invalid status on compare content for {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Comparing content failed for " + replicationConfig, null,
                            null);
                }
                @SuppressWarnings("unchecked") List<String> remotelyDifferentPaths =
                        (List<String>) compareContentState.data(Status.DATA).get(RemoteReceiverConstants.PARAM_PATH);
                LOG.info("Remotely different paths: {}", remotelyDifferentPaths);

                Set<String> pathsToTransmit = new LinkedHashSet<>(remotelyDifferentPaths);
                pathsToTransmit.addAll(contentState.getVersionables().getChangedPaths());
                Set<String> deletedPaths = new LinkedHashSet<>(contentState.getVersionables().getDeletedPaths());
                pathsToTransmit.addAll(deletedPaths); // to synchronize parents
                int count = 0;
                for (String path : pathsToTransmit) {
                    abortIfNecessary(updateInfo); // might be a performance risk, but is probably worth it.
                    progress = 100 * (count++) / pathsToTransmit.size();

                    Resource resource = resolver.getResource(path);
                    if (resource == null) { // we need to transmit the parent nodes of even deleted resources
                        deletedPaths.add(path);
                        resource = new NonExistingResource(resolver, path);
                    }

                    Status status = publisher.pathupload(updateInfo, resource);
                    if (status == null || !status.isValid()) {
                        LOG.error("Received invalid status on pathupload {} : {}", path, status);
                        throw new ReplicationFailedException("Remote upload failed for " + replicationConfig + " " +
                                "path " + path, null, null);
                    }
                }

                abortIfNecessary(updateInfo);
                progress = 99;

                Stream<ChildrenOrderInfo> relevantOrderings = relevantOrderings(changedPaths);

                Status status = publisher.commitUpdate(updateInfo, deletedPaths, relevantOrderings,
                        () -> abortIfNecessary(updateInfo));
                progress = 100;
                if (!status.isValid()) {
                    LOG.error("Received invalid status on commit {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Remote commit failed for " + replicationConfig, null, null);
                } else {
                    cleanupUpdateInfo = null;
                }

                LOG.info("Replication done {}", updateInfo.updateId);
            } catch (RuntimeException | RemotePublicationReceiverFacade.RemotePublicationFacadeException | URISyntaxException e) {
                LOG.error("Remote publishing failed: " + e, e);
                throw new ReplicationFailedException("Remote publishing failed for " + replicationConfig, e, null);
            } finally {
                if (cleanupUpdateInfo != null) { // remove temporary directory.
                    try {
                        abort(cleanupUpdateInfo);
                    } catch (Exception e) {
                        LOG.error("Error cleaning up {}", cleanupUpdateInfo.updateId, e);
                    }
                }
            }
        }

        /**
         * Returns childnode orderings of all parent nodes of {pathsToTransmit} and, if any of pathsToTransmit
         * has versionables as subnodes, of their parent nodes, too. (This needs to work for a full release sync, too.)
         * <p>
         * (There is one edge case we ignore deliberately: if a page was moved several times without any successful sync,
         * that might change some parent orderings there. If it's moved again and now it's synced, we might miss something.
         * That's a rare case which we could catch only if all node orderings are transmitted on each change, which
         * we hesitate to do for efficiency.)
         */
        @Nonnull
        protected Stream<ChildrenOrderInfo> relevantOrderings(Collection<String> pathsToTransmit) {
            Stream<Resource> parentsStream = pathsToTransmit.stream()
                    .flatMap(this::parentsUpToRelease)
                    .distinct()
                    .map(resolver::getResource);
            Stream<Resource> childrenStream = pathsToTransmit.stream()
                    .distinct()
                    .map(resolver::getResource)
                    .filter(Objects::nonNull)
                    .flatMap(this::childrenExcludingVersionables);
            return Stream.concat(parentsStream, childrenStream)
                    .map(ChildrenOrderInfo::of)
                    .filter(Objects::nonNull);
        }

        @Nonnull
        protected Stream<String> parentsUpToRelease(String path) {
            List<String> result = new ArrayList<>();
            String parent = ResourceUtil.getParent(path);
            while (parent != null && isSameOrDescendant(release.getReleaseRoot().getPath(), parent)) {
                result.add(parent);
                parent = ResourceUtil.getParent(parent);
            }
            return result.stream();
        }

        @Nonnull
        protected Stream<Resource> childrenExcludingVersionables(Resource resource) {
            if (resource == null || ResourceUtil.isNodeType(resource, ResourceUtil.TYPE_VERSIONABLE)) {
                return Stream.empty();
            }
            return Stream.concat(Stream.of(resource),
                    StreamSupport.stream(resource.getChildren().spliterator(), false)
                            .flatMap(this::childrenExcludingVersionables));
        }

        protected void abortIfNecessary(UpdateInfo updateInfo) throws RemotePublicationReceiverFacade.RemotePublicationFacadeException, ReplicationFailedException {
            if (abortAtNextPossibility) {
                LOG.info("Aborting because process was interrupted.");
                abort(updateInfo);
                throw new ReplicationFailedException("Aborted publishing because process was interrupted.", null,
                        null);
            }
            release.getMetaDataNode().getResourceResolver().refresh();
            if (!release.getChangeNumber().equals(originalReleaseChangeNumber)) {
                LOG.info("Aborting publishing because of local release content change during publishing.");
                abort(updateInfo);
                throw new ReplicationFailedException("Aborted publishing because of local release content change " +
                        "during publishing.", null, null);
            }
        }

        protected void abort(UpdateInfo updateInfo) throws RemotePublicationReceiverFacade.RemotePublicationFacadeException {
            Status status = publisher.abortUpdate(updateInfo);
            if (status == null || !status.isValid()) {
                LOG.error("Aborting replication failed for {} - please manually clean up resources used there.",
                        updateInfo);
            }
        }

    }

    @ObjectClassDefinition(
            name = "Composum Platform Remote Publisher Service Configuration",
            description = "Configures a service that publishes release changes to remote systems"
    )
    public @interface Configuration {

        @AttributeDefinition(
                description = "the general on/off switch for this service"
        )
        boolean enabled() default false;

        @AttributeDefinition(
                description = "Password to decrypt the passwords in the configurations of remote receivers."
        )
        @Nonnull
        String configurationPassword() default "";

    }

}
