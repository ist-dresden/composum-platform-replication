package com.composum.replication.remotereceiver;

import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.impl.NodeTreeSynchronizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.util.function.Supplier;

/** Base class for remotely updating content on a publish server. */
public abstract class AbstractContentUpdateOperation implements ServletOperation {

    @Nonnull
    protected final Supplier<RemotePublicationReceiverServlet.Configuration> configSupplier;

    @Nonnull
    protected final ResourceResolverFactory resolverFactory;

    /** Name of the {@link Status#data(String)}. */
    public static final String DATAFIELD_NAME = "updateInfo";

    /** Name of the parameter to contain the update id. */
    public static final String PARAM_UPDATEID = "updateId";

    protected final NodeTreeSynchronizer nodeTreeSynchronizer = new NodeTreeSynchronizer();

    public AbstractContentUpdateOperation(
            @Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
            @Nonnull ResourceResolverFactory resolverFactory) {
        this.configSupplier = getConfig;
        this.resolverFactory = resolverFactory;
    }

    @Nonnull
    protected Resource getTmpLocation(@Nonnull ResourceResolver resolver, @Nonnull String updateId, boolean create) throws RepositoryException {
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
        }
        return tmpLocation;
    }

    /** Creates the service resolver used to update the content. */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

}
