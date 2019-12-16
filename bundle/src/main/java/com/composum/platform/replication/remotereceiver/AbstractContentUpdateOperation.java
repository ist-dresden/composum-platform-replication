package com.composum.platform.replication.remotereceiver;

import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import com.composum.sling.platform.staging.impl.NodeTreeSynchronizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.util.function.Supplier;

/** Base class for remotely updating content on a publish server. */
abstract class AbstractContentUpdateOperation implements ServletOperation {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractContentUpdateOperation.class);

    @Nonnull
    protected final Supplier<RemotePublicationReceiverServlet.Configuration> configSupplier;

    @Nonnull
    protected final ResourceResolverFactory resolverFactory;

    protected final NodeTreeSynchronizer nodeTreeSynchronizer = new NodeTreeSynchronizer();

    public AbstractContentUpdateOperation(
            @Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
            @Nonnull ResourceResolverFactory resolverFactory) {
        this.configSupplier = getConfig;
        this.resolverFactory = resolverFactory;
    }

    @Nonnull
    protected Resource getTmpLocation(@Nonnull ResourceResolver resolver, @Nonnull String updateId, boolean create,
                                      Status status) throws RepositoryException {
        if (StringUtils.isBlank(updateId) || updateId.matches("[a-z0-9-]*")) {
            throw new IllegalArgumentException("Broken updateId: " + updateId);
        }
        String path = configSupplier.get().tmpDir() + "/" + updateId;
        Resource tmpLocation = resolver.getResource(path);
        if (tmpLocation == null) {
            if (create) {
                tmpLocation = ResourceUtil.getOrCreateResource(resolver, path);
            } else {
                throw new IllegalArgumentException("Unknown updateId " + updateId);
            }
        } else {
            ValueMap vm = tmpLocation.getValueMap();
            String releaseRootPath = vm.get(RemoteReceiverConstants.ATTR_RELEASEROOT_PATH, String.class);
            String originalReleaseChangeId = vm.get(RemoteReceiverConstants.ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, String.class);
            String releaseChangeId = getReleaseChangeId(resolver, releaseRootPath);
            if (releaseChangeId != null && ! StringUtils.equals(releaseChangeId, originalReleaseChangeId)) {
                status.error("Release change Id changed since beginning of update - aborting transfer. Retryable.");
                LoggerFactory.getLogger(getClass()).error("Release change id changed since beginning of update: {} to" +
                                " {} . Aborting.", originalReleaseChangeId, releaseChangeId);
                throw new IllegalStateException("Release change Id changed since beginning of update - aborting " +
                        "transfer. Retryable.");
                // FIXME(hps,16.12.19) how to transmit that it's retryable for automatic retry?
            }

        }
        return tmpLocation;
    }

    /** Creates the service resolver used to update the content. */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    protected String getReleaseChangeId(ResourceResolver resolver, String contentPath) {
        Resource resource = resolver.getResource(contentPath);
        return resource != null ? resource.getValueMap().get(StagingConstants.PROP_REPLICATED_VERSION, String.class) : null;
    }
}
