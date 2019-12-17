package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.json.JSonOnTheFlyCollectionAdapter;
import com.composum.platform.replication.remote.VersionableInfo;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.AbstractServiceServlet;
import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.servlet.ServletOperationSet;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.fs.io.Importer;
import org.apache.jackrabbit.vault.fs.io.ZipStreamArchive;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.servlets.HttpConstants;
import org.apache.sling.api.servlets.ServletResolverConstants;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_CONTENTPATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_RELEASEROOT_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_UPDATEDPATHS;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_UPDATEID;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PATTERN_UPDATEID;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Component(service = Servlet.class,
        property = {
                Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Publication Receiver Servlet",
                ServletResolverConstants.SLING_SERVLET_PATHS + "=/bin/cpm/platform/replication/publishreceiver",
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_GET,
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_POST,
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_PUT
        })
public class RemotePublicationReceiverServlet extends AbstractServiceServlet {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverServlet.class);

    public enum Extension {zip, json}

    public enum Operation {replaceContent, contentstate, startupdate, commitupdate, pathupload}

    protected final ServletOperationSet<Extension, Operation> operations = new ServletOperationSet<>(Extension.json);

    @Reference
    protected ResourceResolverFactory resolverFactory;

    @Reference
    protected RemotePublicationReceiver service;

    @Override
    protected boolean isEnabled() {
        return service.isEnabled();
    }

    @Override
    protected ServletOperationSet getOperations() {
        return operations;
    }

    @Override
    public void init() throws ServletException {
        super.init();

        operations.setOperation(ServletOperationSet.Method.POST, Extension.zip, Operation.replaceContent,
                new ReplaceContentOperation(service::getConfiguration, resolverFactory));
        operations.setOperation(ServletOperationSet.Method.GET, Extension.json, Operation.contentstate,
                new ContentStateOperation());
        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.startupdate,
                new StartUpdateOperation());
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.json, Operation.pathupload,
                new PathUploadOperation());
    }

    /** Creates the service resolver used to update the content. */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    /**
     * Returns the state of the content of a subtree of a site or the whole site as JSON, including parent node
     * orderings and child node version uuids.
     */
    class ContentStateOperation implements ServletOperation {

        /**
         * Status data variable that contains an array of the versions of all versionables below the given path,
         * in the order they appear in a resource scan.
         */
        public static final String STATUSDATA_VERSIONABLES = "versionables";

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


    }

    /**
     * Extends Status to write data about all versionables below resource without needing to save everything in
     * memory - the data is fetched on the fly during JSON serialization.
     */
    // todo not serializable so far...
    protected class ContentStateStatus extends Status {

        protected transient ResourceHandle resource;

        /**
         * Is serialized to the stream of versionables; needs to be named exactly as
         * {@value #STATUSDATA_VERSIONABLES}.
         */
        // TODO this is not the nicest design, and not deserializable.
        protected JSonOnTheFlyCollectionAdapter.OnTheFlyProducer<VersionableInfo> versionables =
                JSonOnTheFlyCollectionAdapter.onTheFlyProducer((output) -> service.traverseTree(resource, output));

        public ContentStateStatus(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response) {
            super(request, response);
        }

    }

    /** Creates a temporary directory to unpack stuff to replace our content. */
    class StartUpdateOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle ignored)
                throws RepositoryException, IOException, ServletException {
            StatusWithReleaseData status = new StatusWithReleaseData(request, response);
            String contentPath = request.getRequestPathInfo().getSuffix();
            String releaseRootPath = request.getParameter(RemoteReceiverConstants.PARAM_RELEASEROOT);
            if (isNotBlank(releaseRootPath) && isNotBlank(contentPath) &&
                    SlingResourceUtil.isSameOrDescendant(releaseRootPath, contentPath)) {
                try {
                    UpdateInfo updateInfo = service.startUpdate(releaseRootPath, contentPath);
                    status.updateInfo = updateInfo;
                } catch (LoginException e) { // serious misconfiguration
                    LOG.error("Could not get service resolver" + e, e);
                    throw new ServletException("Could not get service resolver", e);
                } catch (RemotePublicationReceiver.RemotePublicationReceiverException e) {
                    status.withLogging(LOG).error("Error starting update for {} , {} : {}", contentPath, releaseRootPath,
                            e.getMessage());
                }
            } else {
                status.withLogging(LOG).error("Broken parameters {} : {}", releaseRootPath, contentPath);
            }
            status.sendJson();
        }

    }

    /** Reads the result of {@link StartUpdateOperation} into memory. */
    public static class StatusWithReleaseData extends Status {

        /** The created update data. */
        UpdateInfo updateInfo;

        public StatusWithReleaseData() {
            super(null, null);
        }

        public StatusWithReleaseData(SlingHttpServletRequest request, SlingHttpServletResponse response) {
            super(request, response);
        }

        @Override
        public boolean isValid() {
            return super.isValid() && updateInfo != null && updateInfo.updateId != null;
        }
    }

    /** Receives a package and saves it in the temporary folder. */
    class PathUploadOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource) throws RepositoryException, IOException, ServletException {
            Status status = new Status(request, response);
            String packageRootPath = request.getRequestPathInfo().getSuffix();
            String updateId = status.getRequiredParameter(PARAM_UPDATEID, PATTERN_UPDATEID, "PatternId required");
            if (isNotBlank(packageRootPath) && status.isValid()) {

                try {
                    service.pathUpload(updateId, packageRootPath, request.getInputStream());
                } catch (LoginException e) { // serious misconfiguration
                    LOG.error("Could not get service resolver" + e, e);
                    throw new ServletException("Could not get service resolver", e);
                } catch (ConfigurationException e) { // on importer.run
                    LOG.error("" + e, e);
                    status.error("Import failed.", e);
                } catch (RemotePublicationReceiver.RemotePublicationReceiverException e) {
                    LOG.error("" + e, e);
                    status.error("Import failed: {}", e.getMessage());
                }
            } else {
                status.withLogging(LOG).error("Broken parameters pkg {}, upd {}", packageRootPath, updateId);
            }
            status.sendJson();
        }
    }
}
