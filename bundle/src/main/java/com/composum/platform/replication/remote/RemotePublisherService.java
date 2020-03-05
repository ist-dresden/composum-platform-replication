package com.composum.platform.replication.remote;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.util.CachedCalculation;
import com.composum.platform.replication.json.ChildrenOrderInfo;
import com.composum.platform.replication.json.NodeAttributeComparisonInfo;
import com.composum.platform.replication.json.VersionableTree;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade.RemotePublicationFacadeException;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.platform.replication.remotereceiver.UpdateInfo;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.logging.Message;
import com.composum.sling.core.logging.MessageContainer;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.platform.staging.ReleaseChangeEventListener;
import com.composum.sling.platform.staging.ReleaseChangeEventPublisher;
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
import org.osgi.service.component.annotations.ConfigurationPolicy;
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
import javax.jcr.RepositoryException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_ATTRIBUTEINFOS;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_CHILDORDERINGS;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_PATH;
import static com.composum.sling.core.util.SlingResourceUtil.isSameOrDescendant;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.awaiting;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.error;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.idle;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.success;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Transmits the changes of the JCR content of a release to a remote system.
 * We transmit the subtrees of all resources changed in the event as a zip to the
 * {@link RemotePublicationReceiverServlet}.
 */
@Component(
        service = ReleaseChangeEventListener.class,
        property = {Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Publisher Service"},
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        immediate = true
)
@Designate(ocd = RemotePublisherService.Configuration.class)
public class RemotePublisherService implements ReleaseChangeEventListener {

    protected static final Logger LOG = LoggerFactory.getLogger(RemotePublisherService.class);

    public static final String PATH_CONFIGROOT = "/conf";
    public static final String DIR_REPLICATION = "/replication";

    /**
     * {@link ReleaseChangeProcess#getType()} for this kind of replication.
     */
    public static final String TYPE_REMOTE = "Remote";

    protected volatile Configuration config;

    protected volatile CloseableHttpClient httpClient;

    @Reference
    protected NodesConfiguration nodesConfig;

    @Reference
    protected ThreadPoolManager threadPoolManager;

    @Reference
    protected StagingReleaseManager releaseManager;

    @Reference
    protected ProxyManagerService proxyManagerService;

    @Reference
    protected CredentialService credentialService;

    @Reference
    protected ResourceResolverFactory resolverFactory;

    protected final Map<String, RemoteReleasePublishingProcess> processesCache = Collections.synchronizedMap(new HashMap<>());

    @Nonnull
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
    public Collection<RemoteReleasePublishingProcess> processesFor(@Nullable Resource resource) {
        if (resource == null || !isEnabled()) {
            return Collections.emptyList();
        }

        ResourceResolver resolver = resource.getResourceResolver();
        Resource releaseRoot;
        try {
            releaseRoot = releaseManager.findReleaseRoot(resource);
        } catch (StagingReleaseManager.ReleaseRootNotFoundException e) {
            return Collections.emptyList();
        }
        BeanContext context = new BeanContext.Service(resolver);

        List<RemotePublicationConfig> replicationConfigs = getReplicationConfigs(releaseRoot, context);
        Collection<RemoteReleasePublishingProcess> processes = new ArrayList<>();
        for (RemotePublicationConfig replicationConfig : replicationConfigs) {
            RemoteReleasePublishingProcess process = processesCache.computeIfAbsent(replicationConfig.getPath(),
                    (k) -> new RemoteReleasePublishingProcess(releaseRoot, replicationConfig)
            );
            process.readConfig(replicationConfig);
            processes.add(process);
        }
        return processes;
    }

    /**
     * Creates the service resolver used to update the content.
     */
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
                if (replicationConfig != null) {
                    configs.add(replicationConfig);
                }
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
        try (CloseableHttpClient ignored = httpClient) { // just make sure it's closed afterwards
            this.config = null;
            Collection<RemoteReleasePublishingProcess> processes = new ArrayList<>(processesCache.values());
            processesCache.clear();
            boolean hasRunningProcesses = false;
            for (RemoteReleasePublishingProcess process : processes) {
                hasRunningProcesses = process.abort(false) || hasRunningProcesses;
            }
            if (hasRunningProcesses) {
                try { // wait a little to hopefully allow safe shutdown with resource cleanup, removing remote stuff
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // shouldn't happen
                }
                for (RemoteReleasePublishingProcess process : processes) {
                    process.abort(true);
                }
            }
            this.httpClient = null;
        }
    }

    protected boolean isEnabled() {
        RemotePublisherService.Configuration theconfig = this.config;
        boolean enabled = theconfig != null && theconfig.enabled();
        if (!enabled) {
            processesCache.clear();
        }
        return enabled;
    }

    protected class RemoteReleasePublishingProcess implements ReleaseChangeProcess {
        // we deliberately save nothing that refers to resolvers, since this is an object that lives long
        @Nonnull
        protected volatile String configPath;
        @Nonnull
        protected volatile String releaseRootPath;
        protected volatile String name;
        protected volatile String description;
        protected volatile boolean enabled;
        protected volatile Boolean active;
        protected volatile String mark;
        protected volatile MessageContainer messages = new MessageContainer(LOG);
        protected final Object changedPathsChangeLock = new Object();
        protected final CachedCalculation<UpdateInfo, RemotePublicationFacadeException> remoteReleaseInfo;

        @Nonnull
        protected volatile Set<String> changedPaths = new LinkedHashSet<>();
        protected volatile String releaseUuid;
        protected volatile ReleaseChangeProcessorState state = idle;
        protected volatile Long finished;
        protected volatile Long startedAt;
        protected volatile ReplicatorStrategy runningStrategy;
        protected volatile Thread runningThread;

        public RemoteReleasePublishingProcess(@Nonnull Resource releaseRoot, @Nonnull RemotePublicationConfig config) {
            releaseRootPath = releaseRoot.getPath();
            readConfig(config);
            remoteReleaseInfo = new CachedCalculation<>(this::remoteReleaseInfo, 60000);
        }

        /**
         * Called as often as possible to adapt to config changes.
         */
        public void readConfig(@Nonnull RemotePublicationConfig remotePublicationConfig) {
            configPath = requireNonNull(remotePublicationConfig.getPath());
            name = remotePublicationConfig.getName();
            description = remotePublicationConfig.getDescription();
            enabled = remotePublicationConfig.isEnabled();
            mark = remotePublicationConfig.getStage();
            active = null;
        }

        protected UpdateInfo remoteReleaseInfo() throws RemotePublicationFacadeException {
            UpdateInfo result = null;
            try (ResourceResolver serviceResolver = makeResolver()) {
                LOG.info("Querying remote release info of {}", configPath);
                ReplicatorStrategy strategy = makeReplicatorStrategy(serviceResolver, null);
                if (strategy != null) {
                    result = strategy.remoteReleaseInfo();
                }
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Can't get service resolver: " + e, e);
                // ignore - that'll reappear when publishing and treated there
            }
            return result;
        }

        public boolean appliesTo(StagingReleaseManager.Release release) {
            ResourceResolver resolver = release.getReleaseRoot().getResourceResolver();
            RemotePublicationConfig publicationConfig = new BeanContext.Service(resolver).adaptTo(RemotePublicationConfig.class);
            List<String> marks = release.getMarks();
            return publicationConfig != null && publicationConfig.isEnabled() && (
                    marks.contains(publicationConfig.getStage().toLowerCase())
                            || marks.contains(publicationConfig.getStage().toUpperCase()));
        }

        @Override
        public void triggerProcessing(@Nonnull ReleaseChangeEvent event) {
            if (!isEnabled()) {
                return;
            }
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
                if (runningStrategy != null) {
                    runningStrategy.setAbortAtNextPossibility();
                }
                state = ReleaseChangeProcessorState.awaiting;
                startedAt = null;
                finished = null;
                messages = new MessageContainer(LOG);
            }
        }

        /**
         * Removes paths that are contained in other paths.
         */
        @Nonnull
        protected Set<String> cleanupPaths(@Nonnull Iterable<String> paths) {
            Set<String> cleanedPaths = new LinkedHashSet<>();
            for (String path : paths) {
                if (cleanedPaths.stream().anyMatch((p) -> isSameOrDescendant(p, path))) {
                    continue;
                }
                cleanedPaths.removeIf((p) -> isSameOrDescendant(path, p));
                cleanedPaths.add(path);
            }
            return cleanedPaths;
        }

        @Override
        public void run() {
            if (state != ReleaseChangeProcessorState.awaiting || changedPaths.isEmpty() || !isEnabled()) {
                LOG.info("Nothing to do in replication {} state {}", configPath, state);
                return;
            }
            state = ReleaseChangeProcessorState.processing;
            startedAt = System.currentTimeMillis();
            ReplicatorStrategy strategy = null;
            messages.clear();
            try (ResourceResolver serviceResolver = makeResolver()) {

                LOG.info("Starting run of replication {}", configPath);

                Set<String> processedChangedPaths = swapOutChangedPaths();
                try {
                    strategy = makeReplicatorStrategy(serviceResolver, processedChangedPaths);
                    if (strategy == null) {
                        messages.add(Message.error("Cannot create strategy - probably disabled"));
                        return;
                    }
                    abortAlreadyRunningStrategy();

                    runningStrategy = strategy;
                    startedAt = System.currentTimeMillis();
                    try {
                        runningThread = Thread.currentThread();
                        runningStrategy.replicate();
                    } finally {
                        runningThread = null;
                    }
                    state = success;
                    processedChangedPaths.clear();
                } finally {
                    addBackChangedPaths(processedChangedPaths);
                }

            } catch (LoginException e) { // misconfiguration
                messages.add(Message.error("Can't get service resolver"), e);
            } catch (InterruptedException e) {
                LOG.error("Interrupted " + e, e);
                messages.add(Message.warn("Interrupted"), e);
            } catch (ReplicationFailedException | RuntimeException e) {
                messages.add(Message.error("Other error: ", e.toString()), e);
            } finally {
                //noinspection ObjectEquality : reset if there wasn't a new strategy created in the meantime
                if (runningStrategy == strategy) {
                    runningStrategy = null;
                }
                if (state != success && state != awaiting) {
                    state = error;
                }
                finished = System.currentTimeMillis();
                LOG.info("Finished run with {} : {} - @{}", state, configPath, System.identityHashCode(this));
            }
        }

        @Override
        @Nullable
        public ReleaseChangeEventPublisher.CompareResult compareTree(@Nonnull ResourceHandle resource,
                                                                     int details) throws ReplicationFailedException {
            if (!isEnabled()) {
                return null;
            }
            ReplicatorStrategy strategy = makeReplicatorStrategy(resource.getResourceResolver(), Collections.singleton(resource.getPath()));
            if (strategy == null) {
                return null;
            }
            try {
                return strategy.compareTree(details);
            } catch (RemotePublicationFacadeException e) {
                throw new ReplicationFailedException(e.getMessage(), e, null);
            }
        }

        @Nullable
        protected ReplicatorStrategy makeReplicatorStrategy(ResourceResolver serviceResolver, Set<String> processedChangedPaths) {
            Resource configResource = serviceResolver.getResource(configPath);
            RemotePublicationConfig replicationConfig = configResource != null ?
                    new BeanContext.Service(serviceResolver).withResource(configResource)
                            .adaptTo(RemotePublicationConfig.class) : null;
            if (replicationConfig == null || !replicationConfig.isEnabled()) {
                LOG.warn("Disabled / unreadable config, not run: {}", getId());
                return null; // warning - should normally have been caught before
            }

            Resource releaseRoot = requireNonNull(serviceResolver.getResource(releaseRootPath), releaseRootPath);
            StagingReleaseManager.Release release = null;
            if (StringUtils.isNotBlank(releaseUuid)) {
                release = releaseManager.findReleaseByUuid(releaseRoot, releaseUuid);
            } else if (StringUtils.isNotBlank(mark)) {
                release = releaseManager.findReleaseByMark(releaseRoot, mark);
            }
            if (release == null) {
                LOG.warn("No applicable release found for {}", getId());
                return null;
            }
            ResourceResolver releaseResolver = releaseManager.getResolverForRelease(release, null, false);
            BeanContext.Service context = new BeanContext.Service(releaseResolver);
            return new ReplicatorStrategy(processedChangedPaths, release, context, replicationConfig, messages);
        }

        protected void abortAlreadyRunningStrategy() throws InterruptedException {
            if (runningStrategy != null) {
                LOG.error("Bug: Strategy already running in parallel? How can that be? {}", runningStrategy);
                runningStrategy.setAbortAtNextPossibility();
                Thread.sleep(5000);
                if (runningThread != null) {
                    runningThread.interrupt();
                    Thread.sleep(2000);
                }
            }
        }

        protected boolean abort(boolean hard) {
            boolean hasRunningStuff = false;
            synchronized (changedPathsChangeLock) {
                ReplicatorStrategy runningStrategyCopy = runningStrategy;
                if (runningStrategyCopy != null) {
                    if (hard) {
                        runningStrategy = null;
                    }
                    runningStrategyCopy.setAbortAtNextPossibility();
                    hasRunningStuff = true;
                }
                if (hard) {
                    Thread runningThreadCopy = runningThread;
                    if (runningThreadCopy != null) {
                        runningThread = null;
                        runningThreadCopy.interrupt();
                        hasRunningStuff = true;
                    }
                }
            }
            return hasRunningStuff;
        }

        /**
         * Returns the current paths in {@link #changedPaths} resetting {@link #changedPaths}.
         */
        @Nonnull
        protected Set<String> swapOutChangedPaths() {
            Set<String> processedChangedPaths;
            synchronized (changedPathsChangeLock) {
                processedChangedPaths = changedPaths;
                changedPaths = new LinkedHashSet<>();
            }
            return processedChangedPaths;
        }

        /**
         * Adds unprocessed paths which were taken out of {@link #changedPaths} by {@link #swapOutChangedPaths()}
         * back into the {@link #changedPaths}.
         */
        protected void addBackChangedPaths(Set<String> unProcessedChangedPaths) {
            if (!unProcessedChangedPaths.isEmpty()) { // add them back
                synchronized (changedPathsChangeLock) {
                    if (changedPaths.isEmpty()) {
                        changedPaths = unProcessedChangedPaths;
                    } else { // some events arrived in the meantime
                        unProcessedChangedPaths.addAll(changedPaths);
                        changedPaths = cleanupPaths(unProcessedChangedPaths);
                        if (!changedPaths.isEmpty()) {
                            state = awaiting;
                        }
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
                case disabled:
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
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public boolean isActive() {
            if (!enabled || StringUtils.isBlank(mark) || StringUtils.isBlank(releaseRootPath)) {
                return false;
            }
            if (active == null) {
                try (ResourceResolver serviceResolver = makeResolver()) {
                    Resource releaseRoot = serviceResolver.getResource(releaseRootPath);
                    StagingReleaseManager.Release release = releaseRoot != null ? releaseManager.findReleaseByMark(releaseRoot, mark) : null;
                    active = release != null;
                } catch (LoginException e) {
                    LOG.error("Can't get service resolver" + e, e);
                }
            }
            return active;
        }

        @Override
        public String getId() {
            return configPath;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getStage() {
            return mark.toLowerCase();
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public String getType() {
            return TYPE_REMOTE;
        }

        @Override
        public void abort() {
            abort(false);
        }

        @Nullable
        @Override
        public Long getRunStartedAt() {
            return startedAt;
        }

        @Nullable
        @Override
        public Long getRunFinished() {
            return finished;
        }

        @Nonnull
        @Override
        public MessageContainer getMessages() {
            return messages;
        }

        @Nullable
        @Override
        public Long getLastReplicationTimestamp() {
            UpdateInfo updateInfo = null;
            try {
                updateInfo = remoteReleaseInfo.giveValue();
            } catch (RemotePublicationFacadeException e) {
                LOG.error("" + e, e);
            }
            return updateInfo != null ? updateInfo.lastReplication : null;
        }

        @Nullable
        @Override
        public Boolean isSynchronized(@Nonnull ResourceResolver resolver) {
            UpdateInfo updateInfo = null;
            try {
                updateInfo = remoteReleaseInfo.giveValue();
            } catch (RemotePublicationFacadeException e) {
                LOG.error("" + e, e);
            }
            Boolean result = null;
            if (updateInfo != null && isNotBlank(updateInfo.originalPublisherReleaseChangeId)) {
                StagingReleaseManager.Release release = getRelease(resolver);
                if (release != null) {
                    result = StringUtils.equals(release.getChangeNumber(), updateInfo.originalPublisherReleaseChangeId);
                }
            }
            return result;
        }

        protected StagingReleaseManager.Release getRelease(@Nonnull ResourceResolver resolver) {
            Resource releaseRoot = resolver.getResource(this.releaseRootPath);
            if (releaseRoot == null) { // safety check - strange case. Site removed? Inaccessible?
                LOG.warn("Cannot find release root {}", this.releaseRootPath);
            }
            return releaseRoot != null ? releaseManager.findReleaseByMark(releaseRoot, this.mark) : null;
        }

        @Override
        public void updateSynchronized() {
            try {
                remoteReleaseInfo.giveValue(null, true); // updates cache
            } catch (RemotePublicationFacadeException e) {
                LOG.error("" + e, e);
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("id", getId())
                    .append("name", name)
                    .append("state", state)
                    .toString();
        }
    }

    /**
     * Responsible for one replication.
     */
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
        protected final String originalSourceReleaseChangeNumber;
        @Nonnull
        protected final RemotePublicationReceiverFacade publisher;
        @Nonnull
        protected final MessageContainer messages;

        protected volatile int progress;

        /**
         * If set, the replication process is aborted at the next step when this is checked.
         */
        protected volatile boolean abortAtNextPossibility = false;
        protected volatile UpdateInfo cleanupUpdateInfo;

        protected ReplicatorStrategy(@Nonnull Set<String> changedPaths, @Nonnull StagingReleaseManager.Release release,
                                     @Nonnull BeanContext context, @Nonnull RemotePublicationConfig replicationConfig,
                                     @Nonnull MessageContainer messages) {
            this.changedPaths = changedPaths;
            this.release = release;
            this.originalSourceReleaseChangeNumber = release.getChangeNumber();
            this.resolver = context.getResolver();
            this.context = context;
            this.replicationConfig = replicationConfig;
            this.messages = messages;

            publisher = new RemotePublicationReceiverFacade(replicationConfig,
                    context, httpClient, () -> config, nodesConfig, proxyManagerService, credentialService);
        }

        /**
         * Sets a mark that leads to aborting the process at the next step - if an outside interruption is necessary
         * for some reason.
         */
        public void setAbortAtNextPossibility() {
            messages.add(Message.info("Abort requested"));
            abortAtNextPossibility = true;
        }

        public void replicate() throws ReplicationFailedException {
            cleanupUpdateInfo = null;
            try {
                String commonParent = SlingResourceUtil.commonParent(changedPaths);
                LOG.info("Changed paths below {}: {}", commonParent, changedPaths);
                requireNonNull(commonParent);
                progress = 0;

                UpdateInfo updateInfo = publisher.startUpdate(release.getReleaseRoot().getPath(), commonParent);
                cleanupUpdateInfo = updateInfo;
                LOG.info("Received UpdateInfo {}", updateInfo);

                if (originalSourceReleaseChangeNumber.equals(updateInfo.originalPublisherReleaseChangeId)) {
                    messages.add(Message.info("Abort publishing since content on remote system is up to date."));
                    return; // abort is called in finally
                }
                messages.add(Message.info("Update {} started", updateInfo.updateId));

                RemotePublicationReceiverServlet.ContentStateStatus contentState = publisher.contentState(updateInfo,
                        changedPaths, resolver, release.getReleaseRoot().getPath());
                if (!contentState.isValid()) {
                    messages.add(Message.error("Received invalid status on contentState for {}", updateInfo.updateId));
                    throw new ReplicationFailedException("Querying content state failed for " + replicationConfig,
                            null, null);
                }
                messages.add(Message.info("Content difference on remote side: {} , deleted {}",
                        contentState.getVersionables().getChangedPaths(), contentState.getVersionables().getDeletedPaths()));
                abortIfNecessary(updateInfo);

                Status compareContentState = publisher.compareContent(updateInfo, changedPaths, resolver, commonParent);
                if (!compareContentState.isValid()) {
                    messages.add(Message.error("Received invalid status on compare content for {}",
                            updateInfo.updateId));
                    throw new ReplicationFailedException("Comparing content failed for " + replicationConfig, null,
                            null);
                }
                @SuppressWarnings("unchecked") List<String> remotelyDifferentPaths =
                        (List<String>) compareContentState.data(Status.DATA).get(PARAM_PATH);
                messages.add(Message.info("Remotely different paths: {}", remotelyDifferentPaths));

                Set<String> pathsToTransmit = new LinkedHashSet<>(remotelyDifferentPaths);
                pathsToTransmit.addAll(contentState.getVersionables().getChangedPaths());
                Set<String> deletedPaths = new LinkedHashSet<>(contentState.getVersionables().getDeletedPaths());
                pathsToTransmit.addAll(deletedPaths); // to synchronize parents
                int count = 0;
                for (String path : pathsToTransmit) {
                    abortIfNecessary(updateInfo);
                    ++count;
                    progress = 89 * (count) / pathsToTransmit.size();

                    Resource resource = resolver.getResource(path);
                    if (resource == null) { // we need to transmit the parent nodes of even deleted resources
                        deletedPaths.add(path);
                        resource = new NonExistingResource(resolver, path);
                    }

                    Status status = publisher.pathupload(updateInfo, resource);
                    if (status == null || !status.isValid()) {
                        messages.add(Message.error("Received invalid status on pathupload {} : {}", path, status));
                        throw new ReplicationFailedException("Remote upload failed for " + replicationConfig + " " +
                                "path " + path, null, null);
                    } else {
                        messages.add(Message.debug("Uploaded {} for {}", path, updateInfo.updateId));
                    }
                }

                abortIfNecessary(updateInfo);
                progress = 90;

                Stream<ChildrenOrderInfo> relevantOrderings = relevantOrderings(changedPaths);

                Status status = publisher.commitUpdate(updateInfo, originalSourceReleaseChangeNumber, deletedPaths,
                        relevantOrderings, () -> abortIfNecessary(updateInfo));
                if (!status.isValid()) {
                    messages.add(Message.error("Received invalid status on commit {}", updateInfo.updateId));
                    progress = 0;
                    throw new ReplicationFailedException("Remote commit failed for " + replicationConfig, null, null);
                }
                progress = 100;
                cleanupUpdateInfo = null;

                messages.add(Message.info("Replication done for {}", updateInfo.updateId));
            } catch (RuntimeException | RemotePublicationFacadeException | URISyntaxException | RepositoryException e) {
                messages.add(Message.error("Remote publishing failed for {} because of {}",
                        cleanupUpdateInfo != null ? cleanupUpdateInfo.updateId : "",
                        String.valueOf(e)), e);
                throw new ReplicationFailedException("Remote publishing failed for " + replicationConfig, e, null);
            } finally {
                if (cleanupUpdateInfo != null) { // remove temporary directory.
                    try {
                        abort(cleanupUpdateInfo);
                    } catch (Exception e) { // notify user since temporary directory can ge huge
                        messages.add(Message.error("Error cleaning up {}", cleanupUpdateInfo.updateId), e);
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
        protected Stream<ChildrenOrderInfo> relevantOrderings(@Nonnull Collection<String> pathsToTransmit) {
            Stream<Resource> relevantNodes = relevantParentNodesOfVersionables(pathsToTransmit);
            return relevantNodes
                    .map(ChildrenOrderInfo::of)
                    .filter(Objects::nonNull);
        }

        /**
         * The attribute infos for all parent nodes of versionables within pathsToTransmit within the release root.
         */
        @Nonnull
        protected Stream<NodeAttributeComparisonInfo> parentAttributeInfos(@Nonnull Collection<String> pathsToTransmit) {
            return relevantParentNodesOfVersionables(pathsToTransmit)
                    .map(resource -> NodeAttributeComparisonInfo.of(resource, null))
                    .filter(Objects::nonNull);
        }

        /**
         * Stream of all parent nodes of the versionables within pathsToTransmit that are within the release root.
         * This consists of the parent nodes of pathsToTransmit and the children of pathsToTransmit (including
         * themselves) up to (and excluding) the versionables.
         */
        @Nonnull
        protected Stream<Resource> relevantParentNodesOfVersionables(@Nonnull Collection<String> pathsToTransmit) {
            Stream<Resource> parentsStream = pathsToTransmit.stream()
                    .flatMap(this::parentsUpToRelease)
                    .distinct()
                    .map(resolver::getResource)
                    .filter(Objects::nonNull);
            Stream<Resource> childrenStream = pathsToTransmit.stream()
                    .distinct()
                    .map(resolver::getResource)
                    .filter(Objects::nonNull)
                    .flatMap(this::childrenExcludingVersionables);
            return Stream.concat(parentsStream, childrenStream);
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

        protected void abortIfNecessary(@Nonnull UpdateInfo updateInfo) throws RemotePublicationFacadeException,
                ReplicationFailedException {
            if (abortAtNextPossibility) {
                messages.add(Message.info("Aborting because that was requested: {}", updateInfo.updateId));
                abort(updateInfo);
                throw new ReplicationFailedException("Aborted publishing because that was requested.", null,
                        null);
            }
            release.getMetaDataNode().getResourceResolver().refresh(); // might be a performance risk (?), but necessary
            if (!release.getChangeNumber().equals(originalSourceReleaseChangeNumber)) {
                messages.add(Message.info("Aborting {} because of local release content change during " +
                        "publishing.", updateInfo.updateId));
                abort(updateInfo);
                throw new ReplicationFailedException("Abort publishing because of local release content change " +
                        "during publishing.", null, null);
            }
        }

        protected void abort(@Nonnull UpdateInfo updateInfo) throws RemotePublicationFacadeException {
            Status status = null;
            try {
                status = publisher.abortUpdate(updateInfo);
                if (status == null || !status.isValid()) {
                    messages.add(Message.error("Aborting replication failed for {} - " +
                            "please manually clean up resources used there.", updateInfo.updateId));
                } else if (cleanupUpdateInfo == updateInfo) {
                    cleanupUpdateInfo = null;
                }
            } catch (RepositoryException e) {
                throw new RemotePublicationReceiverFacade.RemotePublicationFacadeException("Error calling abort", e, status, null);
            }

        }

        @Nullable
        public UpdateInfo remoteReleaseInfo() throws RemotePublicationFacadeException {
            if (!isEnabled()) {
                return null;
            }
            RemotePublicationReceiverServlet.StatusWithReleaseData status = null;
            try {
                status = publisher.releaseInfo(release.getReleaseRoot().getPath());
            } catch (RepositoryException e) {
                throw new RemotePublicationReceiverFacade.RemotePublicationFacadeException("Error calling " +
                        "releaseInfo", e, status, null);
            }
            if (status == null || !status.isValid()) {
                LOG.error("Retrieve remote releaseinfo failed for {}", this.replicationConfig);
                messages.add(Message.error("Retrieve remote releaseinfo failed."));
                return null;
            }
            return status.updateInfo;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ReplicatorStrategy{");
            sb.append("id=").append(replicationConfig.getPath());
            if (replicationConfig != null) {
                sb.append(", receiver=").append(replicationConfig.getTargetUrl());
            }
            if (cleanupUpdateInfo != null) {
                sb.append(", updateInfo=").append(cleanupUpdateInfo.updateId);
            }
            sb.append('}');
            return sb.toString();
        }

        @Nullable
        public ReleaseChangeEventPublisher.CompareResult compareTree(int details) throws RemotePublicationFacadeException, ReplicationFailedException {
            if (!isEnabled()) {
                return null;
            }
            try {
                ReleaseChangeEventPublisher.CompareResult result = new ReleaseChangeEventPublisher.CompareResult();
                RemotePublicationReceiverServlet.StatusWithReleaseData releaseInfoStatus = publisher.releaseInfo(release.getReleaseRoot().getPath());
                UpdateInfo updateInfo = releaseInfoStatus.updateInfo;
                if (!releaseInfoStatus.isValid() || updateInfo == null) {
                    LOG.error("Retrieve remote releaseinfo failed for {}", this.replicationConfig);
                    return null;
                }
                result.releaseChangeNumbersEqual = StringUtils.equals(release.getChangeNumber(),
                        updateInfo.originalPublisherReleaseChangeId);

                // get info on the remote versionables and check which are changed / not present here
                String commonParent = SlingResourceUtil.commonParent(changedPaths);
                RemotePublicationReceiverServlet.ContentStateStatus contentState =
                        publisher.contentState(updateInfo, changedPaths, resolver, commonParent);
                if (!contentState.isValid() || contentState.getVersionables() == null) {
                    throw new ReplicationFailedException("Querying content state failed for " + replicationConfig + " " +
                            "path " + commonParent, null, null);
                }
                VersionableTree contentStateComparison = contentState.getVersionables();

                // check which of our versionables are changed / not present on the remote
                Status compareContentState = publisher.compareContent(updateInfo, changedPaths, resolver, commonParent);
                if (!compareContentState.isValid()) {
                    throw new ReplicationFailedException("Comparing content failed for " + replicationConfig, null,
                            null);
                }
                @SuppressWarnings("unchecked") List<String> remotelyDifferentPaths =
                        (List<String>) compareContentState.data(Status.DATA).get(PARAM_PATH);

                Set<String> differentPaths = new LinkedHashSet<>();
                differentPaths.addAll(remotelyDifferentPaths);
                differentPaths.addAll(contentStateComparison.getDeletedPaths());
                differentPaths.addAll(contentStateComparison.getChangedPaths());
                result.differentVersionablesCount = differentPaths.size();
                if (details > 0) {
                    result.differentVersionables = differentPaths.toArray(new String[0]);
                }

                // compare the children orderings and parent attributes
                Stream<ChildrenOrderInfo> relevantOrderings = relevantOrderings(changedPaths);
                Stream<NodeAttributeComparisonInfo> attributeInfos = parentAttributeInfos(changedPaths);
                Status compareParentState = publisher.compareParents(release.getReleaseRoot().getPath(), resolver,
                        relevantOrderings, attributeInfos);
                if (!compareParentState.isValid()) {
                    throw new ReplicationFailedException("Comparing parents failed for " + replicationConfig, null,
                            null);
                }
                List<String> differentChildorderings = (List<String>) compareParentState.data(PARAM_CHILDORDERINGS).get(PARAM_PATH);
                List<String> changedAttributes = (List<String>) compareParentState.data(PARAM_ATTRIBUTEINFOS).get(PARAM_PATH);
                result.changedChildrenOrderCount = differentChildorderings.size();
                result.changedParentNodeCount = changedAttributes.size();
                if (details > 0) {
                    result.changedChildrenOrders = differentChildorderings.toArray(new String[0]);
                    result.changedParentNodes = changedAttributes.toArray(new String[0]);
                }

                // repeat releaseInfo since this might have taken a while and there might have been a change
                releaseInfoStatus = publisher.releaseInfo(release.getReleaseRoot().getPath());
                if (!releaseInfoStatus.isValid() || updateInfo == null) {
                    LOG.error("Retrieve remote releaseinfo failed for {}", this.replicationConfig);
                    return null;
                }
                result.releaseChangeNumbersEqual = result.releaseChangeNumbersEqual &&
                        StringUtils.equals(release.getChangeNumber(), updateInfo.originalPublisherReleaseChangeId);

                result.equal = result.calculateEqual();
                return result;
            } catch (URISyntaxException | RepositoryException e) {
                LOG.error("" + e, e);
                throw new ReplicationFailedException("Internal error", e, null);
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

    }

}
