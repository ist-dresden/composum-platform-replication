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
import com.composum.sling.platform.staging.replication.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Transmits the changes of the JCR content of a release to a remote system.
 * We transmit the subtrees of all resources changed in the event as a zip to the
 * {@link RemotePublicationReceiverServlet}.
 */
@Component(
        service = ReleaseChangeEventListener.class,
        property = {Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Replication Service"},
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        immediate = true
)
@Designate(ocd = RemotePublisherService.Configuration.class)
public class RemotePublisherService
        extends AbstractReplicationService<RemotePublicationConfig, RemoteReleasePublishingProcess> {

    protected static final Logger LOG = LoggerFactory.getLogger(RemotePublisherService.class);

    protected volatile Configuration config;

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
    protected RemoteReleasePublishingProcess makePublishingProcess(Resource releaseRoot, RemotePublicationConfig replicationConfig) {
        return new RemoteReleasePublishingProcess(releaseRoot, replicationConfig);
    }

    @Activate
    @Modified
    protected void activate(final Configuration theConfig) {
        LOG.info("activated");
        this.config = theConfig;
    }

    @Nonnull
    @Override
    protected Class<RemotePublicationConfig> getReplicationConfigClass() {
        return RemotePublicationConfig.class;
    }

    @Nonnull
    @Override
    protected ReplicationType getReplicationType() {
        return RemotePublicationConfig.REMOTE_REPLICATION_TYPE;
    }

    @Override
    @Deactivate
    protected void deactivate() throws IOException {
        LOG.info("deactivated");
        this.config = null;
        super.deactivate();
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
    protected StagingReleaseManager getReleaseManager() {
        return releaseManager;
    }

    @Override
    protected ResourceResolverFactory getResolverFactory() {
        return resolverFactory;
    }

    protected class RemoteReleasePublishingProcess extends AbstractReplicationProcess implements ReleaseChangeProcess {
        protected final CachedCalculation<UpdateInfo, ReplicationException> remoteReleaseInfo;

        public RemoteReleasePublishingProcess(@Nonnull Resource releaseRoot, @Nonnull RemotePublicationConfig config) {
            super(releaseRoot, config);
            remoteReleaseInfo = new CachedCalculation<>(this::remoteReleaseInfo, 60000);
        }

        @Nonnull
        @Override
        protected PublicationReceiverFacade createTargetFacade(@Nonnull AbstractReplicationConfig replicationConfig, @Nonnull BeanContext context) {
            CloseableHttpClient httpClient = createHttpClient();
            return new RemotePublicationReceiverFacade((RemotePublicationConfig) replicationConfig,
                    context, httpClient, () -> config, nodesConfig, proxyManagerService, credentialService);
        }

        @Override
        public String getType() {
            return getReplicationType().getServiceId();
        }

        /**
         * False: remote replication is never implicit.
         */
        @Override
        public boolean isImplicit() {
            return false;
        }

        @Override
        protected UpdateInfo getTargetReleaseInfo() {
            try {
                return remoteReleaseInfo.giveValue();
            } catch (ReplicationException e) {
                LOG.error("" + e, e);
                return null;
            }
        }

        @Override
        public void updateSynchronized() {
            try {
                remoteReleaseInfo.giveValue(null, true); // updates cache
            } catch (ReplicationException e) {
                LOG.error("" + e, e);
            }
        }

    }

    /**
     * Use different http clients for each replication to avoid sharing cookies etc. - each replication can have a different user.
     */
    protected CloseableHttpClient createHttpClient() {
        return HttpClients.createSystem();
    }

    @ObjectClassDefinition(
            name = "Composum Platform Remote Replication Service Configuration",
            description = "Configures a service that publishes release changes to remote systems"
    )
    public @interface Configuration {

        @AttributeDefinition(
                description = "the general on/off switch for this service"
        )
        boolean enabled() default false;

    }

}
