package com.composum.platform.replication.remote;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverFacade;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.platform.replication.remotereceiver.StartUpdateOperation;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.platform.staging.ReleaseChangeEventListener;
import com.composum.sling.platform.staging.StagingReleaseManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transmits the changes of the JCR content of a release to a remote system.
 * We transmit the subtrees of all resources changed in the event as a zip to the
 * {@link RemotePublicationReceiverServlet}.
 */
@Component(
        service = ReleaseChangeEventListener.class,
        name = "Composum Platform Remote Publisher Service",
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

    protected ThreadPool threadPool;

    @Override
    public void receive(ReleaseChangeEvent event) throws ReplicationFailedException {
        if (!isEnabled()) { return; }
        StagingReleaseManager.Release release = event.release();
        ResourceResolver resolver = releaseManager.getResolverForRelease(release, null, false);
        BeanContext context = new BeanContext.Service(resolver);

        List<String> marks = release.getMarks();
        if (marks.isEmpty()) { return; }

        List<RemotePublicationConfig> replicationConfigs = getReplicationConfigs(release.getReleaseRoot(), context);
        LOG.debug("Replication configurations: {}", replicationConfigs);
        if (replicationConfigs.isEmpty()) { return; }

        for (RemotePublicationConfig replicationConfig : replicationConfigs) {
            if (!replicationConfig.isEnabled() || StringUtils.isBlank(replicationConfig.getReleaseMark())
                    || (!marks.contains(replicationConfig.getReleaseMark().toLowerCase())
                    && !marks.contains(replicationConfig.getReleaseMark().toUpperCase()))) {
                continue;
            }

            new ReplicatorStrategy(event, release, context, replicationConfig).replicate();
        }
    }

    protected List<RemotePublicationConfig> getReplicationConfigs(@Nonnull Resource releaseRoot,
                                                                  @Nonnull BeanContext context) {
        String releasePath = releaseRoot.getPath();
        if (releasePath.startsWith("/content/")) { releasePath = StringUtils.removeStart(releasePath, "/content"); }
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
        this.threadPool = threadPoolManager.get(RemotePublisherService.class.getName());
    }

    @Deactivate
    protected void deactivate() throws IOException {
        LOG.info("deactivated");
        try (CloseableHttpClient ignored = httpClient) { // just make sure it's closed.
            ThreadPool oldThreadPool = this.threadPool;
            this.config = null;
            this.httpClient = null;
            this.threadPool = null;
            if (oldThreadPool != null) { threadPoolManager.release(threadPool); }
        }
    }

    protected boolean isEnabled() {
        RemotePublisherService.Configuration theconfig = this.config;
        return theconfig != null && theconfig.enabled();
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
        String configurationPassword() default "";

    }

    /** Responsible for one replication. */
    protected class ReplicatorStrategy {
        protected final ReleaseChangeEvent event;
        protected final StagingReleaseManager.Release release;
        protected final ResourceResolver resolver;
        protected final BeanContext context;
        protected final RemotePublicationConfig replicationConfig;

        protected ReplicatorStrategy(ReleaseChangeEvent event, StagingReleaseManager.Release release, BeanContext context, RemotePublicationConfig replicationConfig) {
            this.event = event;
            this.release = release;
            this.resolver = context.getResolver();
            this.context = context;
            this.replicationConfig = replicationConfig;
        }

        protected void replicate() throws ReplicationFailedException {
            try {
                Set<String> changedPaths = changedPaths(event);
                String commonParent = SlingResourceUtil.commonParent(changedPaths);
                LOG.info("Changed paths below {}: {}", commonParent, changedPaths);

                RemotePublicationReceiverFacade publisher = new RemotePublicationReceiverFacade(replicationConfig,
                        cryptoService, httpClient, () -> config);

                StartUpdateOperation.UpdateInfo updateInfo =
                        publisher.startUpdate(release.getReleaseRoot().getPath(), commonParent);
                LOG.info("Received UpdateInfo {}", updateInfo);

            } catch (RuntimeException | RemotePublicationReceiverFacade.RemotePublicationReceiverException e) {
                LOG.error("Remote publishing failed: " + e, e);
                throw new ReplicationFailedException("Remote publishing failed for " + replicationConfig, e, event);
            }
        }

        protected Set<String> changedPaths(ReleaseChangeEvent event) {
            Set<String> changedPaths = new LinkedHashSet<>();
            changedPaths.addAll(event.newOrMovedResources());
            changedPaths.addAll(event.removedOrMovedResources());
            changedPaths.addAll(event.updatedResources());
            changedPaths = cleanupPaths(changedPaths);
            return changedPaths;
        }

        /** Removes paths that are contained in other paths. */
        protected Set<String> cleanupPaths(Set<String> changedPaths) {
            Set<String> cleanedPaths = new LinkedHashSet<>();
            for (String path : changedPaths) {
                cleanedPaths.removeIf((p) -> SlingResourceUtil.isSameOrDescendant(path, p));
                if (cleanedPaths.stream().noneMatch((p) -> SlingResourceUtil.isSameOrDescendant(p, path))) {
                    cleanedPaths.add(path);
                }
            }
            return cleanedPaths;
        }

    }
}
