package com.composum.platform.replication.remote;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.util.CachedCalculation;
import com.composum.platform.replication.remote.RemotePublisherService.RemoteReleasePublishingProcess;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.sling.core.BeanContext;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.platform.staging.ReleaseChangeEventListener;
import com.composum.sling.platform.staging.ReleaseChangeProcess;
import com.composum.sling.platform.staging.StagingReleaseManager;
import com.composum.sling.platform.staging.replication.AbstractReplicationService;
import com.composum.sling.platform.staging.replication.PublicationReceiverFacade;
import com.composum.sling.platform.staging.replication.PublicationReceiverFacade.PublicationReceiverFacadeException;
import com.composum.sling.platform.staging.replication.ReplicatorStrategy;
import com.composum.sling.platform.staging.replication.UpdateInfo;
import org.apache.commons.lang3.StringUtils;
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
public class RemotePublisherService extends AbstractReplicationService<RemoteReleasePublishingProcess>
        implements ReleaseChangeEventListener {

    protected static final Logger LOG = LoggerFactory.getLogger(RemotePublisherService.class);

    /**
     * {@link ReleaseChangeProcess#getType()} for this kind of replication.
     */
    public static final String TYPE_REMOTE = "Remote";

    protected volatile Configuration config;

    protected volatile CloseableHttpClient httpClient;

    @Reference
    private ThreadPoolManager threadPoolManager;

    @Reference
    private StagingReleaseManager releaseManager;

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Reference
    protected NodesConfiguration nodesConfig;

    @Reference
    protected ProxyManagerService proxyManagerService;

    @Reference
    protected CredentialService credentialService;

    @Nonnull
    @Override
    public Collection<RemoteReleasePublishingProcess> processesFor(@Nullable Resource resource) {
        if (resource == null || !isEnabled()) {
            return Collections.emptyList();
        }

        ResourceResolver resolver = resource.getResourceResolver();
        Resource releaseRoot;
        try {
            releaseRoot = getReleaseManager().findReleaseRoot(resource);
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

    @Override
    @Deactivate
    protected void deactivate() throws IOException {
        LOG.info("deactivated");
        try (CloseableHttpClient ignored = httpClient) { // just make sure it's closed afterwards
            this.config = null;
            super.deactivate();
            this.httpClient = null;
        }
    }

    @Override
    protected boolean isEnabled() {
        RemotePublisherService.Configuration theconfig = this.config;
        boolean enabled = theconfig != null && theconfig.enabled();
        if (!enabled) {
            processesCache.clear();
        }
        return enabled;
    }

    @Override
    protected ThreadPoolManager getThreadPoolManager() {
        return threadPoolManager;
    }

    @Override
    protected StagingReleaseManager getReleaseManager() {
        return releaseManager;
    }

    @Override
    protected ResourceResolverFactory getResolverFactory() {
        return resolverFactory;
    }

    protected class RemoteReleasePublishingProcess extends AbstractReplicationProcess implements ReleaseChangeProcess {
        protected final CachedCalculation<UpdateInfo, PublicationReceiverFacadeException> remoteReleaseInfo;

        public RemoteReleasePublishingProcess(@Nonnull Resource releaseRoot, @Nonnull RemotePublicationConfig config) {
            super(releaseRoot);
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

        protected UpdateInfo remoteReleaseInfo() throws RemotePublicationReceiverFacade.PublicationReceiverFacadeException {
            if (!isEnabled()) {
                return null;
            }
            UpdateInfo result = null;
            try (ResourceResolver serviceResolver = makeResolver()) {
                LOG.info("Querying remote release info of {}", getId());
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

        @Override
        public boolean appliesTo(StagingReleaseManager.Release release) {
            ResourceResolver resolver = release.getReleaseRoot().getResourceResolver();
            RemotePublicationConfig publicationConfig = new BeanContext.Service(resolver).adaptTo(RemotePublicationConfig.class);
            List<String> marks = release.getMarks();
            return publicationConfig != null && publicationConfig.isEnabled() && (
                    marks.contains(publicationConfig.getStage().toLowerCase())
                            || marks.contains(publicationConfig.getStage().toUpperCase()));
        }

        @Override
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
                release = getReleaseManager().findReleaseByUuid(releaseRoot, releaseUuid);
            } else if (StringUtils.isNotBlank(mark)) {
                release = getReleaseManager().findReleaseByMark(releaseRoot, mark);
            }
            if (release == null) {
                LOG.warn("No applicable release found for {}", getId());
                return null;
            }
            ResourceResolver releaseResolver = getReleaseManager().getResolverForRelease(release, null, false);
            BeanContext.Service context = new BeanContext.Service(releaseResolver);

            PublicationReceiverFacade publisher = new RemotePublicationReceiverFacade(replicationConfig,
                    context, httpClient, () -> config, nodesConfig, proxyManagerService, credentialService);
            return new ReplicatorStrategy(processedChangedPaths, release, context, replicationConfig, messages, publisher);
        }

        @Override
        public String getType() {
            return TYPE_REMOTE;
        }

        @Override
        protected UpdateInfo getTargetReleaseInfo() {
            try {
                return remoteReleaseInfo.giveValue();
            } catch (RemotePublicationReceiverFacade.PublicationReceiverFacadeException e) {
                LOG.error("" + e, e);
                return null;
            }
        }

        @Override
        public void updateSynchronized() {
            try {
                remoteReleaseInfo.giveValue(null, true); // updates cache
            } catch (PublicationReceiverFacadeException e) {
                LOG.error("" + e, e);
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
