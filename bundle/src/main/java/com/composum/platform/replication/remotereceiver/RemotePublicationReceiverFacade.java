package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.json.JsonHttpEntity;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.util.ExceptionThrowingRunnable;
import com.composum.platform.commons.util.ExceptionUtil;
import com.composum.sling.platform.staging.replication.UpdateInfo;
import com.composum.sling.platform.staging.replication.json.ChildrenOrderInfo;
import com.composum.sling.platform.staging.replication.json.NodeAttributeComparisonInfo;
import com.composum.sling.platform.staging.replication.json.VersionableTree;
import com.composum.platform.replication.remote.RemotePublisherService;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.LinkUtil;
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
import javax.jcr.RepositoryException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

/**
 * Provides a Java interface for accessing the remote publication receiver.
 */
public class RemotePublicationReceiverFacade implements PublicationReceiverFacade {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverFacade.class);

    @Nonnull
    protected final BeanContext context;

    @Nonnull
    protected final RemotePublicationConfig replicationConfig;

    @Nonnull
    protected final NodesConfiguration nodesConfig;

    @Nonnull
    protected final ProxyManagerService proxyManagerService;

    @Nonnull
    protected final CredentialService credentialService;

    @Nonnull
    protected final Supplier<RemotePublisherService.Configuration> generalConfig;

    @Nonnull
    protected final CloseableHttpClient httpClient;

    public RemotePublicationReceiverFacade(@Nonnull RemotePublicationConfig replicationConfig,
                                           @Nonnull BeanContext context,
                                           @Nonnull CloseableHttpClient httpClient,
                                           @Nonnull Supplier<RemotePublisherService.Configuration> generalConfig,
                                           @Nonnull NodesConfiguration nodesConfiguration,
                                           @Nonnull ProxyManagerService proxyManagerService,
                                           @Nonnull CredentialService credentialService
    ) {
        this.context = context;
        this.replicationConfig = replicationConfig;
        this.nodesConfig = nodesConfiguration;
        this.generalConfig = generalConfig;
        this.httpClient = httpClient;
        this.proxyManagerService = proxyManagerService;
        this.credentialService = credentialService;
    }

    protected URIBuilder uriBuilder(Operation operation, Extension ext, String path) throws URISyntaxException {
        return new URIBuilder(uriString(operation, ext, path));
    }

    protected String uriString(Operation operation, Extension ext, String path) {
        return uriString(operation, ext) + LinkUtil.encodePath(path);
    }

    protected String uriString(Operation operation, Extension ext) {
        return replicationConfig.getTargetUrl() + "." + operation.name() + "." + ext.name();
    }

    @Override
    @Nonnull
    public StatusWithReleaseData startUpdate(@NotNull String releaseRoot, @Nonnull String path) throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_RELEASEROOT, releaseRoot));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(startUpdate, json, path);
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Start update in release {} for path {}", releaseRoot, path);
        StatusWithReleaseData status =
                callRemotePublicationReceiver("Starting update with " + path,
                        httpClientContext, post, StatusWithReleaseData.class, null);
        if (status.updateInfo == null || StringUtils.isBlank(status.updateInfo.updateId)) { // impossible
            throw ExceptionUtil.logAndThrow(LOG,
                    new PublicationReceiverFacadeException("Received no updateId for " + path, null, status, null));
        }
        return status;
    }

    @Override
    @Nonnull
    public StatusWithReleaseData releaseInfo(@NotNull String releaseRootPath) throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        String uri = uriString(releaseInfo, json, releaseRootPath);
        HttpGet method = new HttpGet(uri);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Get releaseinfo for path {}", releaseRootPath);
        }
        StatusWithReleaseData status =
                callRemotePublicationReceiver("Get releaseinfo for " + releaseRootPath,
                        httpClientContext, method, StatusWithReleaseData.class, null);
        return status;
    }

    @Override
    @Nonnull
    public ContentStateStatus contentState(
            @Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths, ResourceResolver resolver, String contentPath)
            throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        for (String path : paths) {
            form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_PATH, path));
        }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(contentState, json);
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Querying content state for {} , {}", updateInfo.updateId, paths);
        Gson gson = new GsonBuilder().registerTypeAdapterFactory(
                new VersionableTree.VersionableTreeDeserializer(null, resolver, contentPath)
        ).create();
        ContentStateStatus status =
                callRemotePublicationReceiver("Querying content for " + paths,
                        httpClientContext, post, ContentStateStatus.class, gson);
        return status;
    }

    @Override
    @Nonnull
    public Status compareContent(@Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths,
                                 ResourceResolver resolver, String contentPath)
            throws URISyntaxException, PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        URI uri = uriBuilder(compareContent, json, contentPath)
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

    @Override
    @Nonnull
    public Status pathupload(@Nonnull UpdateInfo updateInfo, @Nonnull Resource resource) throws PublicationReceiverFacadeException, URISyntaxException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        URI uri = uriBuilder(pathUpload, zip, resource.getPath())
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId).build();
        HttpPut put = new HttpPut(uri);
        put.setEntity(new PackageHttpEntity(nodesConfig, context, resource));

        LOG.info("Uploading package for {}", SlingResourceUtil.getPath(resource));
        Status status = callRemotePublicationReceiver("pathupload " + resource.getPath(),
                httpClientContext, put, Status.class, null);
        return status;
    }

    @Override
    @Nonnull
    public Status commitUpdate(@Nonnull UpdateInfo updateInfo, @Nonnull String newReleaseChangeNumber,
                               @Nonnull Set<String> deletedPaths,
                               @Nonnull Stream<ChildrenOrderInfo> relevantOrderings,
                               @Nonnull ExceptionThrowingRunnable<? extends Exception> checkForParallelModifications)
            throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        Gson gson = new GsonBuilder().create();

        HttpEntity entity = new JsonHttpEntity(null, null) {
            @Override
            protected void writeTo(@Nonnull JsonWriter jsonWriter) throws IOException {
                jsonWriter.beginObject();
                jsonWriter.name(RemoteReceiverConstants.PARAM_UPDATEID).value(updateInfo.updateId);
                jsonWriter.name(RemoteReceiverConstants.PARAM_RELEASE_CHANGENUMBER).value(newReleaseChangeNumber);
                jsonWriter.name(RemoteReceiverConstants.PARAM_DELETED_PATH).beginArray();
                for (String deletedPath : deletedPaths) {
                    jsonWriter.value(deletedPath);
                }
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

        String uri = uriString(commitUpdate, json);
        HttpPut put = new HttpPut(uri);
        put.setEntity(entity);

        LOG.info("Comitting update {} deleting {}", updateInfo.updateId, deletedPaths);
        Status status = callRemotePublicationReceiver("Committing update " + updateInfo.updateId,
                httpClientContext, put, Status.class, null);
        return status;
    }

    @Override
    @Nonnull
    public Status abortUpdate(@Nonnull UpdateInfo updateInfo) throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(abortUpdate, json);
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Aborting update for {}", updateInfo);
        Status status =
                callRemotePublicationReceiver("Aborting update of " + updateInfo.updateId,
                        httpClientContext, post, Status.class, null);
        return status;
    }

    @Override
    public Status compareParents(String releaseRoot, ResourceResolver resolver, Stream<ChildrenOrderInfo> relevantOrderings,
                                 Stream<NodeAttributeComparisonInfo> attributeInfos) throws PublicationReceiverFacadeException, RepositoryException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                proxyManagerService, credentialService);
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

        String uri = uriString(compareParents, json, releaseRoot);
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
            @Nonnull Class<T> statusClass, @Nullable Gson gson) throws PublicationReceiverFacadeException {
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remote call successful about {} with {}, {}", logmessage,
                            statusLine.getStatusCode(), statusLine.getReasonPhrase());
                }
            } else {
                throw ExceptionUtil.logAndThrow(LOG,
                        new PublicationReceiverFacadeException("Received invalid status from remote system for " + logmessage,
                                null, status, statusLine));
            }
        } catch (IOException e) {
            throw ExceptionUtil.logAndThrow(LOG, new PublicationReceiverFacadeException(
                    "Trouble accessing remote service for " + logmessage, e, status,
                    statusLine));
        }
        return status;
    }

}
