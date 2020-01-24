package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.platform.commons.util.ExceptionThrowingRunnable;
import com.composum.platform.commons.util.ExceptionUtil;
import com.composum.platform.replication.json.ChildrenOrderInfo;
import com.composum.platform.replication.json.NodeAttributeComparisonInfo;
import com.composum.platform.replication.json.VersionableTree;
import com.composum.platform.replication.remote.RemotePublisherService;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension.json;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension.zip;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.abortUpdate;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.commitUpdate;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.compareContent;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.compareParents;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.contentState;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.pathUpload;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.releaseInfo;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.startUpdate;

/** Provides a Java interface for accessing the remote publication receiver. */
public class RemotePublicationReceiverFacade {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverFacade.class);

    @Nonnull
    protected final BeanContext context;

    @Nonnull
    protected final RemotePublicationConfig replicationConfig;

    @Nonnull
    protected final CryptoService cryptoService;

    @Nonnull
    protected final NodesConfiguration nodesConfig;

    @Nonnull
    protected final Supplier<RemotePublisherService.Configuration> generalConfig;

    @Nonnull
    protected final CloseableHttpClient httpClient;

    public RemotePublicationReceiverFacade(@Nonnull RemotePublicationConfig replicationConfig,
                                           @Nonnull BeanContext context,
                                           @Nonnull CloseableHttpClient httpClient,
                                           @Nonnull Supplier<RemotePublisherService.Configuration> generalConfig,
                                           @Nonnull CryptoService cryptoService,
                                           @Nonnull NodesConfiguration nodesConfiguration
    ) {
        this.context = context;
        this.replicationConfig = replicationConfig;
        this.cryptoService = cryptoService;
        this.nodesConfig = nodesConfiguration;
        this.generalConfig = generalConfig;
        this.httpClient = httpClient;
    }

    public Function<String, String> passwordDecryptor() {
        Function<String, String> decryptor = null;
        String key = generalConfig.get().configurationPassword();
        if (StringUtils.isNotBlank(key)) {
            decryptor = (password) -> cryptoService.decrypt(password, key);
        }
        return decryptor;
    }

    /**
     * Starts an update process on the remote side. To clean up resources, either
     * {@link #commitUpdate(UpdateInfo, Set, Stream, ExceptionThrowingRunnable)} or
     * {@link #abortUpdate(UpdateInfo)} must be called afterwards.
     *
     * @param releaseRoot the root of the release containing {path} (may be equal to {path})
     * @param path        the root content path that should be considered. Might be the root of a release, or any
     *                    subdirectory.
     * @return the basic information about the update which must be used for all related calls on this update.
     */
    @Nonnull
    public UpdateInfo startUpdate(@NotNull String releaseRoot, @Nonnull String path) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_RELEASEROOT, releaseRoot));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + startUpdate.name() + "." + json.name() + path;
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Start update in release {} for path {}", releaseRoot, path);
        RemotePublicationReceiverServlet.StatusWithReleaseData status =
                callRemotePublicationReceiver("Starting update with " + path,
                        httpClientContext, post, RemotePublicationReceiverServlet.StatusWithReleaseData.class, null);
        if (status.updateInfo == null || StringUtils.isBlank(status.updateInfo.updateId)) { // impossible
            throw ExceptionUtil.logAndThrow(LOG,
                    new RemotePublicationFacadeException("Received no updateId for " + path, null, status, null));
        }
        return status.updateInfo;
    }

    /**
     * Starts an update process on the remote side. To clean up resources, either
     * {@link #commitUpdate(UpdateInfo, Set, Stream, ExceptionThrowingRunnable)} or
     * {@link #abortUpdate(UpdateInfo)} must be called afterwards.
     *
     * @param path            the root content path that should be considered. Might be the root of a release, or any
     *                        subdirectory.
     * @param releaseRootPath the root of the release containing {path} (may be equal to {path})
     * @return the basic information about the update which must be used for all related calls on this update.
     */
    @Nonnull
    public RemotePublicationReceiverServlet.StatusWithReleaseData releaseInfo(@NotNull String releaseRootPath) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        String uri = replicationConfig.getReceiverUri() + "." + releaseInfo.name() + "." + json.name() + releaseRootPath;
        HttpGet method = new HttpGet(uri);

        LOG.info("Get releaseinfo for path {}", releaseRootPath);
        RemotePublicationReceiverServlet.StatusWithReleaseData status =
                callRemotePublicationReceiver("Get releaseinfo for " + releaseRootPath,
                        httpClientContext, method, RemotePublicationReceiverServlet.StatusWithReleaseData.class, null);
        return status;
    }

    /**
     * Queries the versions of versionables below {paths} on the remote side and returns in the status which
     * resources of the remote side have a different version and which do not exist.
     *
     * @param paths       the paths to query
     * @param contentPath a path that is a common parent to all paths - just a safety feature that a broken / faked
     *                    response cannot compare unwanted areas of the content.
     */
    @Nonnull
    public RemotePublicationReceiverServlet.ContentStateStatus contentState(
            @Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths, ResourceResolver resolver, String contentPath)
            throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        for (String path : paths) { form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_PATH, path)); }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + contentState.name() + "." + json.name();
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Querying content state for {} , {}", updateInfo.updateId, paths);
        Gson gson = new GsonBuilder().registerTypeAdapterFactory(
                new VersionableTree.VersionableTreeDeserializer(null, resolver, contentPath)
        ).create();
        RemotePublicationReceiverServlet.ContentStateStatus status =
                callRemotePublicationReceiver("Querying content for " + paths,
                        httpClientContext, post, RemotePublicationReceiverServlet.ContentStateStatus.class, gson);
        return status;
    }

    /**
     * Transmits the versions of versionables below {paths} to the remote side, which returns a list of paths
     * that have different versions or do not exists with {@link Status#data(String)}({@value Status#DATA}) attribute
     * {@link RemoteReceiverConstants#PARAM_PATH} as List&lt;String>.
     */
    @Nonnull
    public Status compareContent(@Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths,
                                 ResourceResolver resolver, String contentPath)
            throws URISyntaxException, RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        URI uri = new URIBuilder(replicationConfig.getReceiverUri() + "." + compareContent.name() +
                "." + json.name() + contentPath)
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId)
                .build();
        HttpPut put = new HttpPut(uri);
        Gson gson = new GsonBuilder().registerTypeAdapterFactory(
                new VersionableTree.VersionableTreeSerializer(null)
        ).create();
        VersionableTree versionableTree = new VersionableTree();
        Collection<Resource> resources = paths.stream()
                .map(resolver::getResource)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        versionableTree.setSearchtreeRoots(resources);
        put.setEntity(new JsonHttpEntity<>(versionableTree, gson));

        LOG.info("Comparing content for {}", paths);
        Status status = callRemotePublicationReceiver("compare content " + paths,
                httpClientContext, put, Status.class, null);
        return status;
    }

    /** Uploads the resource tree to the remote machine. */
    @Nonnull
    public Status pathupload(@Nonnull UpdateInfo updateInfo, @Nonnull Resource resource) throws RemotePublicationFacadeException, URISyntaxException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        URI uri =
                new URIBuilder(replicationConfig.getReceiverUri() + "." + pathUpload.name() + "." + zip.name() + resource.getPath())
                        .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId).build();
        HttpPut put = new HttpPut(uri);
        put.setEntity(new PackageHttpEntity(nodesConfig, context, resource));

        LOG.info("Uploading package for {}", SlingResourceUtil.getPath(resource));
        Status status = callRemotePublicationReceiver("pathupload " + resource.getPath(),
                httpClientContext, put, Status.class, null);
        return status;
    }

    /**
     * Replaces the content with the updated content and deletes obsolete paths.
     *
     * @param checkForParallelModifications executed at the last possible time before the request is completed, to allow
     *                                      checking for parallel modifications of the source
     */
    @Nonnull
    public Status commitUpdate(@Nonnull UpdateInfo updateInfo, @Nonnull String newReleaseChangeNumber,
                               @Nonnull Set<String> deletedPaths,
                               @Nonnull Stream<ChildrenOrderInfo> relevantOrderings,
                               @Nonnull ExceptionThrowingRunnable<? extends Exception> checkForParallelModifications)
            throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(), passwordDecryptor());
        Gson gson = new GsonBuilder().create();

        HttpEntity entity = new JsonHttpEntity(null, null) {
            @Override
            protected void writeTo(@Nonnull JsonWriter jsonWriter) throws IOException {
                jsonWriter.beginObject();
                jsonWriter.name(RemoteReceiverConstants.PARAM_UPDATEID).value(updateInfo.updateId);
                jsonWriter.name(RemoteReceiverConstants.PARAM_RELEASE_CHANGENUMBER).value(newReleaseChangeNumber);
                jsonWriter.name(RemoteReceiverConstants.PARAM_DELETED_PATH).beginArray();
                for (String deletedPath : deletedPaths) { jsonWriter.value(deletedPath); }
                jsonWriter.endArray();
                jsonWriter.name(RemoteReceiverConstants.PARAM_CHILDORDERINGS).beginArray();
                relevantOrderings.forEachOrdered(childrenOrderInfo ->
                        gson.toJson(childrenOrderInfo, childrenOrderInfo.getClass(), jsonWriter));

                jsonWriter.flush();
                // last check that the original was not modified in the meantime, since that might have taken some time.
                try {
                    checkForParallelModifications.run();
                } catch (Exception e) {
                    LOG.warn("Aborting because last check indicates parallel modification.", e);
                    ExceptionUtil.sneakyThrowException(e);
                }

                jsonWriter.endArray();
                jsonWriter.endObject();
            }
        };

        String uri = replicationConfig.getReceiverUri() + "." + commitUpdate.name() + "." + json.name();
        HttpPut put = new HttpPut(uri);
        put.setEntity(entity);

        LOG.info("Comitting update {} deleting {}", updateInfo.updateId, deletedPaths);
        Status status = callRemotePublicationReceiver("Committing update " + updateInfo.updateId,
                httpClientContext, put, Status.class, null);
        return status;
    }

    /** Aborts the update, deleting the temporary directory on the remote side. */
    @Nonnull
    public Status abortUpdate(@Nonnull UpdateInfo updateInfo) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + abortUpdate.name() + "." + json.name();
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Aborting update for {}", updateInfo);
        Status status =
                callRemotePublicationReceiver("Aborting update of " + updateInfo.updateId,
                        httpClientContext, post, Status.class, null);
        return status;
    }

    /** Compares children order and attributes of the parents. */
    public Status compareParents(String releaseRoot, ResourceResolver resolver, Stream<ChildrenOrderInfo> relevantOrderings,
                                 Stream<NodeAttributeComparisonInfo> attributeInfos) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(), passwordDecryptor());
        Gson gson = new GsonBuilder().create();

        HttpEntity entity = new JsonHttpEntity(null, null) {
            @Override
            protected void writeTo(@Nonnull JsonWriter jsonWriter) throws IOException {
                jsonWriter.beginObject();

                jsonWriter.name(RemoteReceiverConstants.PARAM_CHILDORDERINGS).beginArray();
                relevantOrderings.forEachOrdered(childrenOrderInfo ->
                        gson.toJson(childrenOrderInfo, childrenOrderInfo.getClass(), jsonWriter));
                jsonWriter.endArray();

                jsonWriter.name(RemoteReceiverConstants.PARAM_ATTRIBUTEINFOS).beginArray();
                attributeInfos.forEachOrdered(attributeInfo ->
                        gson.toJson(attributeInfo, attributeInfo.getClass(), jsonWriter));
                jsonWriter.endArray();

                jsonWriter.endObject();
            }
        };

        String uri = replicationConfig.getReceiverUri() + "." + compareParents.name() + "." + json.name() + releaseRoot;
        HttpPut put = new HttpPut(uri);
        put.setEntity(entity);

        LOG.info("Comparing parents for {}", releaseRoot);
        Status status = callRemotePublicationReceiver("Comparing parents for " + releaseRoot,
                httpClientContext, put, Status.class, null);
        return status;
    }

    @Nonnull
    protected <T extends Status> T callRemotePublicationReceiver(
            @Nonnull String logmessage, @Nonnull HttpClientContext httpClientContext, @Nonnull HttpUriRequest request,
            @Nonnull Class<T> statusClass, @Nullable Gson gson) throws RemotePublicationFacadeException {
        gson = gson != null ? gson : new GsonBuilder().create();
        T status = null;
        StatusLine statusLine = null;
        try (CloseableHttpResponse response = httpClient.execute(request, httpClientContext)) {
            statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream content = entity.getContent()) {
                    Reader contentReader = new InputStreamReader(content, StandardCharsets.UTF_8);
                    status = gson.fromJson(contentReader, statusClass);
                }
            }
            if (status != null && status.isValid() && status.isSuccess()) {
                LOG.info("Remote call successful about {} with {}, {}", logmessage,
                        statusLine.getStatusCode(), statusLine.getReasonPhrase());
            } else {
                throw ExceptionUtil.logAndThrow(LOG,
                        new RemotePublicationFacadeException("Received invalid status from remote system for " + logmessage,
                                null, status, statusLine));
            }
        } catch (IOException e) {
            throw ExceptionUtil.logAndThrow(LOG, new RemotePublicationFacadeException(
                    "Trouble accessing remote service for " + logmessage, e, status,
                    statusLine));
        }
        return status;
    }

    /** Exception that signifies a problem with the replication. */
    public static class RemotePublicationFacadeException extends Exception {
        protected final Status status;
        protected final Integer statusCode;
        protected final String reasonPhrase;

        public RemotePublicationFacadeException(String message, Throwable throwable, Status status, StatusLine statusLine) {
            super(message, throwable);
            this.status = status;
            this.statusCode = statusLine != null ? statusLine.getStatusCode() : null;
            this.reasonPhrase = statusLine != null ? statusLine.getReasonPhrase() : null;
        }

        @Nullable
        public Status getStatus() {
            return status;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(super.toString()).append("{");
            if (statusCode != null) { sb.append(", statusCode=").append(statusCode); }
            if (reasonPhrase != null) { sb.append(", reasonPhrase='").append(reasonPhrase).append('\''); }
            if (status != null) {
                try (StringWriter statusString = new StringWriter()) {
                    status.toJson(new JsonWriter(statusString));
                    sb.append(", status=").append(statusString.toString());
                } catch (IOException e) {
                    LOG.error("" + e, e);
                    sb.append(", status=Cannot deserialize: ").append(e);
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }

}
