package com.composum.platform.replication.remote;

import com.composum.platform.commons.crypt.CryptoService;
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
import com.composum.sling.platform.staging.StagingReleaseManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.threads.ThreadPool;
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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    protected volatile ThreadPool threadPool;

    @Reference
    protected NodesConfiguration nodesConfig;

    @Reference
    protected ThreadPoolManager threadPoolManager;

    @Reference
    protected StagingReleaseManager releaseManager;

    @Reference
    protected CryptoService cryptoService;

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

    @Nonnull
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
            if (oldThreadPool != null) { threadPoolManager.release(oldThreadPool); }
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
        @Nonnull
        String configurationPassword() default "";

    }

    /** Responsible for one replication. */
    protected class ReplicatorStrategy {
        @Nonnull
        protected final ReleaseChangeEvent event;
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

        protected ReplicatorStrategy(@Nonnull ReleaseChangeEvent event, @Nonnull StagingReleaseManager.Release release,
                                     @Nonnull BeanContext context, @Nonnull RemotePublicationConfig replicationConfig) {
            this.event = event;
            this.release = release;
            this.originalReleaseChangeNumber = release.getChangeNumber();
            this.resolver = context.getResolver();
            this.context = context;
            this.replicationConfig = replicationConfig;

            publisher = new RemotePublicationReceiverFacade(replicationConfig,
                    context, httpClient, () -> config, cryptoService, nodesConfig);
        }

        protected void replicate() throws ReplicationFailedException {
            try {
                Set<String> changedPaths = changedPaths();
                String commonParent = SlingResourceUtil.commonParent(changedPaths);
                LOG.info("Changed paths below {}: {}", commonParent, changedPaths);
                Objects.requireNonNull(commonParent);

                UpdateInfo updateInfo = publisher.startUpdate(release.getReleaseRoot().getPath(), commonParent);
                LOG.info("Received UpdateInfo {}", updateInfo);

                if (originalReleaseChangeNumber.equals(updateInfo.originalPublisherReleaseChangeId)) {
                    LOG.info("Abort publishing since content on remote system is up to date.");
                    abort(publisher, updateInfo);
                    return;
                }

                RemotePublicationReceiverServlet.ContentStateStatus contentState = publisher.contentState(updateInfo,
                        changedPaths, resolver, release.getReleaseRoot().getPath());
                if (!contentState.isValid()) {
                    LOG.error("Received invalid status on contentState for {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Querying content state failed for " + replicationConfig, null, event);
                }
                LOG.info("Content difference on remote side: {} , deleted {}",
                        contentState.getVersionables().getChanged(), contentState.getVersionables().getDeleted());

                Status compareContentState = publisher.compareContent(updateInfo, changedPaths, resolver, commonParent);
                if (!compareContentState.isValid()) {
                    LOG.error("Received invalid status on compare content for {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Comparing content failed for " + replicationConfig, null, event);
                }
                @SuppressWarnings("unchecked") List<String> remotelyDifferentPaths =
                        (List<String>) compareContentState.data(Status.DATA).get(RemoteReceiverConstants.PARAM_PATH);
                LOG.info("Remotely different paths: {}", remotelyDifferentPaths);

                Set<String> pathsToTransmit = new LinkedHashSet<>();
                pathsToTransmit.addAll(remotelyDifferentPaths);
                pathsToTransmit.addAll(contentState.getVersionables().getChangedPaths());
                Set<String> deletedPaths = new LinkedHashSet<>();
                deletedPaths.addAll(contentState.getVersionables().getDeletedPaths());
                for (String path : pathsToTransmit) {
                    abortIfOriginalChanged(updateInfo); // might be a performance risk, but is probably worth it.

                    Resource resource = resolver.getResource(path);
                    if (resource != null) {
                        Status status = publisher.pathupload(updateInfo, resource);
                        if (status == null || !status.isValid()) {
                            LOG.error("Received invalid status on pathupload {} : {}", path, status);
                            throw new ReplicationFailedException("Remote upload failed for " + replicationConfig + " " +
                                    "path " + path, null, event);
                        }
                    } else {
                        deletedPaths.add(path);
                    }
                }

                abortIfOriginalChanged(updateInfo);

                Stream<ChildrenOrderInfo> relevantOrderings = relevantOrderings(changedPaths);

                Status status = publisher.commitUpdate(updateInfo, deletedPaths, relevantOrderings,
                        () -> abortIfOriginalChanged(updateInfo));
                if (!status.isValid()) {
                    LOG.error("Received invalid status on commit {}", updateInfo.updateId);
                    throw new ReplicationFailedException("Remote commit failed for " + replicationConfig, null, event);
                }

                LOG.info("Replication done {}", updateInfo.updateId);
            } catch (RuntimeException | RemotePublicationReceiverFacade.RemotePublicationFacadeException | URISyntaxException e) {
                LOG.error("Remote publishing failed: " + e, e);
                throw new ReplicationFailedException("Remote publishing failed for " + replicationConfig, e, event);
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
            while (parent != null && SlingResourceUtil.isSameOrDescendant(release.getReleaseRoot().getPath(), parent)) {
                result.add(parent);
                parent = ResourceUtil.getParent(parent);
            }
            return result.stream();
        }

        @Nonnull
        protected Stream<Resource> childrenExcludingVersionables(Resource resource) {
            if (resource == null || ResourceUtil.isNodeType(resource, ResourceUtil.TYPE_VERSIONABLE)) { return Stream.empty(); }
            return Stream.concat(Stream.of(resource),
                    StreamSupport.stream(resource.getChildren().spliterator(), false)
                            .flatMap(this::childrenExcludingVersionables));
        }

        protected void abortIfOriginalChanged(UpdateInfo updateInfo) throws RemotePublicationReceiverFacade.RemotePublicationFacadeException, ReplicationFailedException {
            release.getMetaDataNode().getResourceResolver().refresh();
            if (!release.getChangeNumber().equals(originalReleaseChangeNumber)) {
                LOG.info("Aborting publishing because of local release content change during publishing.");
                abort(publisher, updateInfo); // doesn't return
            }
        }

        protected void abort(RemotePublicationReceiverFacade publisher, UpdateInfo updateInfo) throws RemotePublicationReceiverFacade.RemotePublicationFacadeException, ReplicationFailedException {
            Status status = publisher.abortUpdate(updateInfo);
            if (status == null || !status.isValid()) {
                LOG.error("Aborting replication failed for {} - please manually clean up resources used there.",
                        updateInfo);
            }
            throw new ReplicationFailedException("Aborted publishing because of local release content change " +
                    "during publishing.", null, event);
        }

        @Nonnull
        protected Set<String> changedPaths() {
            Set<String> changedPaths = new LinkedHashSet<>();
            changedPaths.addAll(event.newOrMovedResources());
            changedPaths.addAll(event.removedOrMovedResources());
            changedPaths.addAll(event.updatedResources());
            changedPaths = cleanupPaths(changedPaths);
            return changedPaths;
        }

        /** Removes paths that are contained in other paths. */
        @Nonnull
        protected Set<String> cleanupPaths(@Nonnull Iterable<String> changedPaths) {
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
