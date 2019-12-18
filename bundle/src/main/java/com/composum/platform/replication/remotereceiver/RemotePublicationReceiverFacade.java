package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.platform.commons.util.ExceptionUtil;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension.json;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.abortupdate;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.commitupdate;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.comparecontent;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.contentstate;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.pathupload;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.startupdate;

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

    @Nonnull
    protected HttpClientContext makeHttpClientContext() {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        return httpClientContext;
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
     * Starts an update process on the remote side. To clean up resources, either {@link #commitUpdate(String)} or
     * {@link #abortUpdate(String)} must be called afterwards.
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
        String uri = replicationConfig.getReceiverUri() + "." + startupdate.name() + "." + json.name() + path;
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Start update for {} , {}", releaseRoot, path);
        RemotePublicationReceiverServlet.StatusWithReleaseData status =
                callRemotePublicationReceiver("Starting update with " + path,
                        httpClientContext, post, RemotePublicationReceiverServlet.StatusWithReleaseData.class, null);
        UpdateInfo updateInfo = status.updateInfo;
        return updateInfo;
    }

    /**
     * Queries the versions of versionables below {paths} on the remote side and returns in the status which
     * resources of the remote side have a different version and which do not exist.
     *
     * @param paths       the paths to query
     * @param contentPath a path that is a common parent to all paths - just a safety feature that a broken / faked
     *                    response cannot compare unwanted areas of the content.
     */
    @Nullable
    public RemotePublicationReceiverServlet.ContentStateStatus contentState(
            @Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths, ResourceResolver resolver, String contentPath)
            throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        for (String path : paths) { form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_PATH, path)); }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + contentstate.name() + "." + json.name();
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
    @Nullable
    public Status compareContent(@Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths,
                                 ResourceResolver resolver, String contentPath)
            throws URISyntaxException, RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        URI uri = new URIBuilder(replicationConfig.getReceiverUri() + "." + comparecontent.name() + "." + json.name())
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId).build();
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
    public Status pathupload(@Nonnull UpdateInfo updateInfo, @Nonnull Resource resource) throws RemotePublicationFacadeException, URISyntaxException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        URI uri = new URIBuilder(replicationConfig.getReceiverUri() + "." + pathupload.name() + "." + json.name() + resource.getPath())
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId).build();
        HttpPut put = new HttpPut(uri);
        put.setEntity(new PackageHttpEntity(nodesConfig, context, resource));

        LOG.info("Uploading package for {}", SlingResourceUtil.getPath(resource));
        Status status = callRemotePublicationReceiver("pathupload " + resource.getPath(),
                httpClientContext, put, Status.class, null);
        return status;
    }

    /** Replaces the content with the updated content and deletes obsolete paths. */
    @Nonnull
    public Status commitUpdate(@Nonnull UpdateInfo updateInfo, @Nonnull Set<String> deletedPaths) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        for (String deletedPath : deletedPaths) {
            form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_DELETED_PATH, deletedPath));
        }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + commitupdate.name() + "." + json.name();
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Comitting update {} deleting {}", updateInfo.updateId, deletedPaths);
        Status status = callRemotePublicationReceiver("Committing update " + updateInfo.updateId,
                httpClientContext, post, Status.class, null);
        return status;
    }

    /** Aborts the update, deleting the temporary directory on the remote side. */
    public Status abortUpdate(@Nonnull UpdateInfo updateInfo) throws RemotePublicationFacadeException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + abortupdate.name() + "." + json.name();
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Aborting update for {}", updateInfo);
        Status status =
                callRemotePublicationReceiver("Aborting update of " + updateInfo.updateId,
                        httpClientContext, post, Status.class, null);
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
                    Charset charset;
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
                StringWriter statusString = new StringWriter();
                try {
                    status.toJson(new JsonWriter(statusString));
                    sb.append("status=").append(statusString.toString());
                } catch (IOException e) {
                    LOG.error("" + e, e);
                    sb.append("status=").append("Cannot deserialize: " + e);
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }

}
