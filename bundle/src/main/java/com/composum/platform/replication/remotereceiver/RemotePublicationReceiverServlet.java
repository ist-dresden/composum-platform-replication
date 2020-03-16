package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.json.JsonArrayAsIterable;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.AbstractServiceServlet;
import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.servlet.ServletOperationSet;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.core.util.XSS;
import com.composum.sling.platform.staging.replication.PublicationReceiverFacade;
import com.composum.sling.platform.staging.replication.ReplicationPaths;
import com.composum.sling.platform.staging.replication.json.ChildrenOrderInfo;
import com.composum.sling.platform.staging.replication.json.NodeAttributeComparisonInfo;
import com.composum.sling.platform.staging.replication.json.VersionableInfo;
import com.composum.sling.platform.staging.replication.json.VersionableTree;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.*;
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
import java.util.*;
import java.util.stream.Collectors;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.*;
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

    public enum Operation {
        contentState, compareContent, startUpdate, pathUpload, commitUpdate, abortUpdate,
        releaseInfo, compareParents
    }

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

        // we allow both GET and POST for contentstate since it might have many parameters.
        operations.setOperation(ServletOperationSet.Method.GET, Extension.json, Operation.contentState,
                new ContentStateOperation());
        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.contentState,
                new ContentStateOperation());

        // use PUT since request is a potentially large JSON entity processable on the fly
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.json, Operation.compareContent,
                new CompareContentOperation());

        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.startUpdate,
                new StartUpdateOperation());

        // use PUT since request is a stream
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.zip, Operation.pathUpload,
                new PathUploadOperation());

        // use PUT since request is a potentially large JSON entity processable on the fly
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.json, Operation.commitUpdate,
                new CommitUpdateOperation());

        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.abortUpdate,
                new AbortUpdateOperation());

        operations.setOperation(ServletOperationSet.Method.POST, Extension.json, Operation.releaseInfo,
                new ReleaseInfoOperation());
        operations.setOperation(ServletOperationSet.Method.GET, Extension.json, Operation.releaseInfo,
                new ReleaseInfoOperation());

        // use PUT since request is a potentially large JSON entity processable on the fly
        operations.setOperation(ServletOperationSet.Method.PUT, Extension.json, Operation.compareParents,
                new CompareParentsOperation());
    }

    /**
     * Creates the service resolver used to update the content.
     */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    protected void expectName(JsonReader jsonReader, String expectedName, Status status) throws IOException, JsonParseException {
        String nextName = jsonReader.nextName();
        if (!expectedName.equals(nextName)) {
            status.error("{} expected but got {}", expectedName, nextName);
            throw new JsonParseException(" expected " + expectedName + " but got " + nextName);
        }
    }

    /**
     * Returns the state of the content of a subtree of a site or the whole site as JSON, including parent node
     * orderings and child node version uuids.
     */
    class ContentStateOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response,
                         @Nullable ResourceHandle ignoredResource) throws IOException, ServletException {
            // FIXME(hps,11.03.20) move to service. Also this depends on targetPath
            PublicationReceiverFacade.ContentStateStatus status = null;
            try (ResourceResolver resolver = makeResolver()) {
                String chRoot = requireNonNull(service.getChangeRoot());
                ReplicationPaths replicationPaths = new ReplicationPaths(request);
                String contentPath = replicationPaths.getContentPath();
                VersionableTree.VersionableTreeSerializer factory = new VersionableTree.VersionableTreeSerializer(
                        replicationPaths.inverseTranslateMapping(chRoot)
                );
                GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapterFactory(factory);
                status = new PublicationReceiverFacade.ContentStateStatus(gsonBuilder, request, response, LOG);

                try {
                    String[] additionalPaths = XSS.filter(request.getParameterValues(RemoteReceiverConstants.PARAM_PATH));
                    List<String> paths = new ArrayList<>();
                    if (StringUtils.isNotBlank(contentPath)) {
                        paths.add(contentPath);
                    }
                    if (additionalPaths != null) {
                        paths.addAll(Arrays.asList(additionalPaths));
                    }

                    List<Resource> resources = paths.stream()
                            .map(replicationPaths::trimToOrigin)
                            .filter(Objects::nonNull)
                            .map(replicationPaths.translateMapping(chRoot))
                            .map(resolver::getResource)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    status.versionables = new VersionableTree();
                    status.versionables.setSearchtreeRoots(resources);
                } catch (RuntimeException e) {
                    status.withLogging(LOG).error("Error getting content state {} : {}", contentPath, e.toString(), e);
                }
                status.sendJson(); // resolver still has to be open since VersionableTreeSerializer streams data
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver: " + e, e);
                throw new ServletException("Could not get service resolver", e);
            } catch (RuntimeException e) {
                LOG.error("" + e, e);
                throw new ServletException("Internal error", e);
            }
        }


    }

    /**
     * Receives a number of {@link VersionableInfo} in a PUT request and
     * compares them to the current content. The paths that differ or do not exist are returned in the response
     * {@link Status#data(String)}({@value Status#DATA}) attribute {@link RemoteReceiverConstants#PARAM_PATH} as List&lt;String>.
     */
    class CompareContentOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            Status status = new Status(request, response, LOG);
            String updateId = XSS.filter(request.getParameter(PARAM_UPDATEID));
            ReplicationPaths replicationPaths = null;
            try {
                replicationPaths = ReplicationPaths.optional(request);
                List<String> diffpaths = service.compareContent(replicationPaths, updateId, request.getReader());
                status.data(Status.DATA).put(RemoteReceiverConstants.PARAM_PATH, diffpaths);
            } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | PersistenceException | RuntimeException e) {
                status.error("Error comparing content for {} : {}", updateId, e.toString(), e);
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver: " + e, e);
                throw new ServletException("Could not get service resolver", e);
            }
            status.sendJson();
        }
    }

    /**
     * Creates a temporary directory to unpack stuff to replace our content.
     */
    class StartUpdateOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle ignored)
                throws IOException, ServletException {
            PublicationReceiverFacade.StatusWithReleaseData status = new PublicationReceiverFacade.StatusWithReleaseData(request, response, LOG);
            ReplicationPaths replicationPaths = null;
            try {
                replicationPaths = new ReplicationPaths(request);
                status.updateInfo = service.startUpdate(replicationPaths);
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver: " + e, e);
                throw new ServletException("Could not get service resolver", e);
            } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | PersistenceException | RuntimeException e) {
                status.error("Error starting update for {} , {}", replicationPaths, e.toString(), e);
            }
            status.sendJson();
        }

    }

    /**
     * Receives a package and saves it in the temporary folder.
     */
    class PathUploadOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            Status status = new Status(request, response, LOG);
            String packageRootPath = XSS.filter(request.getRequestPathInfo().getSuffix());
            String updateId = status.getRequiredParameter(PARAM_UPDATEID, PATTERN_UPDATEID, "UpdateId required");
            if (isNotBlank(packageRootPath) && status.isValid()) {

                try {
                    service.pathUpload(updateId, packageRootPath, request.getInputStream());
                } catch (LoginException e) { // serious misconfiguration
                    LOG.error("Could not get service resolver:" + e, e);
                    throw new ServletException("Could not get service resolver", e);
                } catch (ConfigurationException e) { // on importer.run
                    LOG.error("" + e, e);
                    status.error("Import failed.", e.toString());
                } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | PersistenceException | RuntimeException e) {
                    LOG.error("" + e, e);
                    status.error("Import failed: {}", e.toString());
                }
            } else {
                status.error("Broken parameters pkg {}, upd {}", packageRootPath, updateId);
            }
            status.sendJson();
        }
    }

    class CommitUpdateOperation implements ServletOperation {
        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            Status status = new Status(request, response, LOG);
            Gson gson = new GsonBuilder().create();

            try (JsonReader jsonReader = new JsonReader(request.getReader())) {
                jsonReader.beginObject();

                expectName(jsonReader, PARAM_UPDATEID, status);
                String updateId = jsonReader.nextString();
                if (!PATTERN_UPDATEID.matcher(updateId).matches()) {
                    status.error("Invalid updateId");
                    throw new IllegalArgumentException("Invalid updateId");
                }

                expectName(jsonReader, PARAM_RELEASE_CHANGENUMBER, status);
                String newReleaseChangeId = jsonReader.nextString();
                if (StringUtils.isBlank(newReleaseChangeId)) {
                    status.error("Missing releaseChangeNumber");
                    throw new IllegalArgumentException("Missing releaseChangeNumber");
                }

                Set<String> deletedPaths = new HashSet<>();
                expectName(jsonReader, PARAM_DELETED_PATH, status);
                jsonReader.beginArray();
                while (jsonReader.hasNext()) {
                    deletedPaths.add(jsonReader.nextString());
                }
                jsonReader.endArray();

                expectName(jsonReader, PARAM_CHILDORDERINGS, status);
                JsonArrayAsIterable<ChildrenOrderInfo> childOrderings =
                        new JsonArrayAsIterable<>(jsonReader, ChildrenOrderInfo.class, gson, null);

                if (status.isValid()) {
                    LOG.info("Commit on {} deleting {}", updateId, deletedPaths);
                    try {
                        service.commit(updateId, deletedPaths, childOrderings, newReleaseChangeId);
                        jsonReader.endObject();
                    } catch (LoginException e) { // serious misconfiguration
                        LOG.error("Could not get service resolver: " + e, e);
                        throw new ServletException("Could not get service resolver", e);
                    } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | PersistenceException | RuntimeException e) {
                        status.error("Import failed for {}: {}", updateId, e.toString(), e);
                    }
                }
            } catch (IOException | RuntimeException e) {
                status.error("Reading request for commit failed", e);
            }

            status.sendJson();
        }

    }

    class AbortUpdateOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            Status status = new Status(request, response, LOG);
            String updateId = status.getRequiredParameter(PARAM_UPDATEID, PATTERN_UPDATEID, "PatternId required");
            LOG.info("Aborting update {}", updateId);
            if (status.isValid()) {
                try {
                    service.abort(updateId);
                } catch (LoginException e) { // serious misconfiguration
                    LOG.error("Could not get service resolver: " + e, e);
                    throw new ServletException("Could not get service resolver", e);
                } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | PersistenceException | RuntimeException e) {
                    status.error("Import failed for {}: {}", updateId, e.toString(), e);
                }
            }
            status.sendJson();
        }
    }

    class ReleaseInfoOperation implements ServletOperation {

        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            ReplicationPaths replicationPaths = null;
            PublicationReceiverFacade.StatusWithReleaseData status = new PublicationReceiverFacade.StatusWithReleaseData(request, response, LOG);
            try {
                replicationPaths = new ReplicationPaths(request);
                status.updateInfo = service.releaseInfo(replicationPaths);
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver: " + e, e);
                throw new ServletException("Could not get service resolver", e);
            } catch (RuntimeException | RepositoryException e) {
                status.error("Cannot get release info for {}: {}", replicationPaths, e.toString(), e);
            }
            status.sendJson();
        }
    }

    class CompareParentsOperation implements ServletOperation {
        @Override
        public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource)
                throws IOException, ServletException {
            Status status = new Status(request, response, LOG);
            Gson gson = new GsonBuilder().create();
            ReplicationPaths replicationPaths = null;
            try (JsonReader jsonReader = new JsonReader(request.getReader())) {
                replicationPaths = new ReplicationPaths(request);
                jsonReader.beginObject();
                expectName(jsonReader, PARAM_REPLICATIONPATHS, status);
                replicationPaths = gson.fromJson(jsonReader, ReplicationPaths.class);

                JsonArrayAsIterable<ChildrenOrderInfo> childOrderings =
                        new JsonArrayAsIterable<>(jsonReader, ChildrenOrderInfo.class, gson, PARAM_CHILDORDERINGS);
                JsonArrayAsIterable<NodeAttributeComparisonInfo> attributeInfos =
                        new JsonArrayAsIterable<>(jsonReader, NodeAttributeComparisonInfo.class, gson,
                                PARAM_ATTRIBUTEINFOS);

                List<String> differentChildorderings = service.compareChildorderings(replicationPaths, childOrderings);
                status.data(PARAM_CHILDORDERINGS).put(PARAM_PATH, differentChildorderings);

                List<String> differentParentAttributes = service.compareAttributes(replicationPaths, attributeInfos);
                status.data(PARAM_ATTRIBUTEINFOS).put(PARAM_PATH, differentParentAttributes);

                jsonReader.endObject();
            } catch (LoginException e) { // serious misconfiguration
                LOG.error("Could not get service resolver: " + e, e);
                throw new ServletException("Could not get service resolver", e);
            } catch (RemotePublicationReceiver.RemotePublicationReceiverException | RepositoryException | RuntimeException | IOException e) {
                status.error("Compare childorderings failed: {}", e.toString(), e);
            }

            status.sendJson();
        }
    }
}
