package com.composum.replication.remote;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Returns the state of the content of a subtree of a site or the whole site as JSON, including parent node
 * orderings and child node version uuids.
 */
public class ContentStateOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ContentStateOperation.class);

    /**
     * Status data variable that contains a map of parent paths of the requested resource to the order of nodes
     * there.
     */
    public static final String STATUSDATA_SIBLINGORDER = "siblingorder";

    public ContentStateOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                                 @Nonnull ResourceResolverFactory resolverFactory) {
        super(getConfig, resolverFactory);
    }

    @Override
    public void doIt(SlingHttpServletRequest request, SlingHttpServletResponse response, ResourceHandle resource) throws RepositoryException, IOException, ServletException {
        Status status = new Status(request, response);
        // FIXME(hps,09.12.19) we possibly should use a service resolver here. Not sure yet. -> Override
        //  AbstractServiceServlet.getResource.
        if (resource.isValid()) {
            Resource releaseRoot = addParentSiblings(resource, status);
            if (releaseRoot == null) {
                status.withLogging(LOG).error("Not in a release root: {}", resource.getPath());
            } else {
                addDescendantVersionableData(resource, status);
            }
        } else {
            status.withLogging(LOG).error("No readable path given as suffix: {}", request.getRequestPathInfo().getSuffix());
        }
        status.sendJson();
    }

    /**
     * Adds the sibling orders of the resource and it's parents up to the
     * {@value com.composum.sling.platform.staging.StagingConstants#TYPE_MIX_RELEASE_ROOT} to the status data,
     * if there are several.
     *
     * @return the release root
     */
    protected Resource addParentSiblings(@Nonnull ResourceHandle resource, @Nonnull Status status) {
        Resource releaseRoot = null;
        if (resource.isOfType(StagingConstants.TYPE_MIX_RELEASE_ROOT)) {
            releaseRoot = resource;
        } else {
            ResourceHandle parent = resource.getParent();
            if (parent != null && parent.isValid()) {
                List<String> childnames = new ArrayList<>();
                for (Resource child : parent.getChildren()) {
                    childnames.add(child.getName());
                }
                if (childnames.size() > 1) {
                    status.data(STATUSDATA_SIBLINGORDER).put(resource.getPath(), childnames);
                }
                releaseRoot = addParentSiblings(parent, status);
            } else { // we hit / - don't transmit anything here.
                status.data(STATUSDATA_SIBLINGORDER).clear();
            }
        }
        return releaseRoot;
    }

    /** Traverses through the descendant tree of resource up to the content nodes and logs the versions of the
     * versionables. */
    protected void addDescendantVersionableData(ResourceHandle resource, Status status) {
        if (resource.isOfType(ResourceUtil.MIX_VERSIONABLE)) {
        }
    }

}
