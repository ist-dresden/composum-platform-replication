package com.composum.platform.replication.remotereceiver;

import com.composum.platform.replication.json.VersionableTree;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.AbstractServiceServlet;
import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.servlet.ServletOperationSet;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.LoginException;
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
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_DELETED_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_UPDATEID;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PATTERN_UPDATEID;
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

    public enum Operation {replaceContent, contentstate, comparecontent, startupdate, pathupload, commitupdate, abortupdate}

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
        // we allow both GET and POST for contentstate since it might have many parameters.
        operations.setOperation(ServletOperationSet.Method.GET, Extension.json, Operation.contentstate,
                new ContentStateOperation());
        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.contentstate,
                new ContentStateOperation());
        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.startupdate,
                new StartUpdateOperation());
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.json, Operation.pathupload,
                new PathUploadOperation());
        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.commitupdate,
                new CommitUpdateOperation());
        // FIXME(hps,17.12.19) abortUpdate
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
            String targetDir = Objects.requireNonNull(service.getTargetDir());
            VersionableTree.VersionableTreeSerializer factory = new VersionableTree.VersionableTreeSerializer(targetDir);
            Gson gson = new GsonBuilder().registerTypeAdapterFactory(factory).create();
            ContentStateStatus status = new ContentStateStatus(gson, request, response);

            String contentPath = request.getRequestPathInfo().getSuffix();
            String[] additionalPaths = request.getParameterValues(RemoteReceiverConstants.PARAM_PATH);
            List<String> paths = new ArrayList<>();
            if (StringUtils.isNotBlank(contentPath)) { paths.add(contentPath); }
            if (additionalPaths != null) { paths.addAll(Arrays.asList(additionalPaths)); }

            try (ResourceResolver resolver = makeResolver()) {
                List<Resource> resources = paths.stream()
                        .map((p) -> SlingResourceUtil.appendPaths(targetDir, p))
                        .map(resolver::getResource)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                status.versionables = new VersionableTree();
                status.versionables.setSearchtreeRoots(resources);
                status.sendJson();
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver" + e, e);
                throw new ServletException("Could not get service resolver", e);
            }
        }


    }

    /**
     * Extends Status to write data about all versionables below resource without needing to save everything in
     * memory - the data is fetched on the fly during JSON serialization.
     */
    public class ContentStateStatus extends Status {

        /** The attribute; need to register serializer - see {@link VersionableTree}. */
        protected VersionableTree versionables;

        public VersionableTree getVersionables() {
            return versionables;
        }

        public ContentStateStatus(@Nonnull final Gson gson, @Nonnull SlingHttpServletRequest request,
                                  @Nonnull SlingHttpServletResponse response) {
            super(gson, request, response);
        }

        /** @deprecated for instantiation by GSon only */
        @Deprecated
        public ContentStateStatus() {
            super(null, null);
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

    class CommitUpdateOperation implements ServletOperation {
        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource) throws RepositoryException, IOException, ServletException {
            Status status = new Status(request, response);
            String updateId = status.getRequiredParameter(PARAM_UPDATEID, PATTERN_UPDATEID, "PatternId required");
            Set<String> deletedPaths = new HashSet<>();
            String[] deletedParms = request.getParameterValues(PARAM_DELETED_PATH);
            if (null != deletedParms) { deletedPaths.addAll(Arrays.asList(deletedParms));}
            LOG.info("Commit on {} deleting {}", updateId, deletedPaths);
            if (status.isValid()) {
                try {
                    service.commit(updateId, deletedPaths);
                } catch (LoginException e) { // serious misconfiguration
                    LOG.error("Could not get service resolver" + e, e);
                    throw new ServletException("Could not get service resolver", e);
                } catch (RemotePublicationReceiver.RemotePublicationReceiverException e) {
                    LOG.error("" + e, e);
                    status.error("Import failed: {}", e.getMessage());
                }
            }
            status.sendJson();
        }

    }
}
