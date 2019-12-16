package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.json.JSonOnTheFlyCollectionAdapter;
import com.composum.platform.replication.remote.VersionableInfo;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.composum.sling.platform.staging.StagingConstants.PROP_REPLICATED_VERSION;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Returns the state of the content of a subtree of a site or the whole site as JSON, including parent node
 * orderings and child node version uuids.
 */
class ContentStateOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ContentStateOperation.class);

    /**
     * Status data variable that contains an array of the versions of all versionables below the given path,
     * in the order they appear in a resource scan.
     */
    public static final String STATUSDATA_VERSIONABLES = "versionables";

    ContentStateOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                          @Nonnull ResourceResolverFactory resolverFactory) {
        super(getConfig, resolverFactory);
    }

    @Override
    public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response,
                     @Nullable ResourceHandle ignoredResource) throws RepositoryException, IOException, ServletException {
        ContentStateStatus status = new ContentStateStatus(request, response);
        String contentPath = request.getRequestPathInfo().getSuffix();
        try (ResourceResolver resolver = makeResolver()) {
            ResourceHandle resource = isNotBlank(contentPath) ? ResourceHandle.use(resolver.getResource(contentPath)) : null;
            if (resource != null && resource.isValid()) {
                status.resource = resource;
            } else {
                status.withLogging(LOG).error("No readable path given as suffix: {}", request.getRequestPathInfo().getSuffix());
            }
        } catch (LoginException e) { // serious misconfiguration
            LOG.error("Could not get service resolver" + e, e);
            throw new ServletException("Could not get service resolver", e);
        }
        status.sendJson();
    }

    /**
     * Adds the sibling orders of the resource and it's parents up to the
     * {@value com.composum.sling.platform.staging.StagingConstants#TYPE_MIX_RELEASE_ROOT} to the data,
     * if there are several.
     *
     * @return the release root
     */
    @Deprecated
    // FIXME(hps,09.12.19) remove this later - probably not needed.
    protected Resource addParentSiblings(@Nonnull ResourceHandle resource, @Nonnull Map<String, List<String>> data) {
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
                    data.put(resource.getPath(), childnames);
                }
                releaseRoot = addParentSiblings(parent, data);
            } else { // we hit / - don't transmit anything here.
                data.clear();
            }
        }
        return releaseRoot;
    }

    /**
     * Extends Status to write data about all versionables below resource without needing to save everything in
     * memory - the data is fetched on the fly during JSON serialization.
     */
    protected class ContentStateStatus extends Status {

        protected transient ResourceHandle resource;

        /**
         * Is serialized to the stream of versionables; needs to be named exactly as
         * {@vale #STATUSDATA_VERSIONABLES}.
         */
        // TODO this is not the nicest design, and not deserializable.
        protected JSonOnTheFlyCollectionAdapter.OnTheFlyProducer<VersionableInfo> versionables =
                JSonOnTheFlyCollectionAdapter.onTheFlyProducer((output) -> traverseTree(resource, output));

        public ContentStateStatus(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response) {
            super(request, response);
        }

        /**
         * Traverses through the descendant tree of resource up to the content nodes and logs the versions of the
         * versionables.
         */
        protected void traverseTree(Resource resource, Consumer<VersionableInfo> output) throws IOException {
            if (resource == null) { return; }
            if (ResourceUtil.isNodeType(resource, ResourceUtil.TYPE_VERSIONABLE)) {
                VersionableInfo info = VersionableInfo.of(resource);
                if (info != null) { output.accept(info); }
            } else if (ResourceUtil.CONTENT_NODE.equals(resource.getName())) {
                // that shouldn't happen in the intended usecase: non-versionable jcr:content
                LOG.warn("Something's wrong here: {} has no {}", resource.getPath(), PROP_REPLICATED_VERSION);
            } else { // traverse tree
                for (Resource child : resource.getChildren()) {
                    traverseTree(child, output);
                }
            }
        }

    }

}
