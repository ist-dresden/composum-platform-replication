package com.composum.replication.remote;

import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.platform.staging.impl.NodeTreeSynchronizer;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/** Base class for remotely updating content on a publish server. */
public abstract class AbstractContentUpdateOperation implements ServletOperation {

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

    /** Creates the service resolver used to update the content. */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

}
