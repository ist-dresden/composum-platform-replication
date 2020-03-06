package com.composum.platform.replication.remote;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.util.CachedCalculation;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade.RemotePublicationFacadeException;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.platform.replication.remotereceiver.UpdateInfo;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.logging.Message;
import com.composum.sling.core.logging.MessageContainer;
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
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.composum.sling.core.util.SlingResourceUtil.isSameOrDescendant;
import static com.composum.sling.platform.staging.ReleaseChangeProcess.ReleaseChangeProcessorState.*;
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
        if (release == null || !isEnabled()) {
            return Collections.emptyList();
        }

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
            if (!isEnabled()) {
                return null;
            }
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

            RemotePublicationReceiverFacade publisher = new RemotePublicationReceiverFacade(replicationConfig,
                    context, httpClient, () -> config, nodesConfig, proxyManagerService, credentialService);
            return new ReplicatorStrategy(processedChangedPaths, release, context, replicationConfig, messages, publisher);
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
