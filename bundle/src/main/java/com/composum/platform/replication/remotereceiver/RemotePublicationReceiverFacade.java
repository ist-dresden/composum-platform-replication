package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.json.JsonHttpEntity;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.util.ExceptionThrowingRunnable;
import com.composum.platform.commons.util.ExceptionUtil;
import com.composum.platform.replication.remote.RemotePublisherService;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.logging.Message;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.LinkUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.platform.staging.replication.*;
import com.composum.sling.platform.staging.replication.json.ChildrenOrderInfo;
import com.composum.sling.platform.staging.replication.json.NodeAttributeComparisonInfo;
import com.composum.sling.platform.staging.replication.json.VersionableTree;
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
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension.json;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Extension.zip;
import static com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet.Operation.*;

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

    protected URIBuilder uriBuilder(Operation operation, Extension ext, String path) throws ReplicationException {
        String url = uriString(operation, ext, path);
        try {
            return new URIBuilder(url);
        } catch (URISyntaxException e) {
            throw new ReplicationException(Message.error("Invalid URL used in replication - please check configuration: {}",
                    url), e);
        }
    }

    protected String uriString(@Nonnull Operation operation, @Nonnull Extension ext, @Nullable String path) {
        return uriString(operation, ext) +
                (StringUtils.isNotBlank(path) ? LinkUtil.encodePath(path) : "");
    }

    protected String uriString(@Nonnull Operation operation, @Nonnull Extension ext) {
        return replicationConfig.getTargetUrl() + "." + operation.name() + "." + ext.name();
    }

    protected URI buildUrl(URIBuilder uriBuilder) throws ReplicationException {
        try {
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new ReplicationException(Message.error("Invalid URL used in replication - please check configuration: {}",
                    uriBuilder.toString()), e);
        }
    }

    @Nonnull
    @Override
    public StatusWithReleaseData startUpdate(@Nonnull ReplicationPaths replicationPaths)
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        List<NameValuePair> form = new ArrayList<>();
        replicationPaths.addToForm(form);
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(startUpdate, json, replicationPaths.getContentPath());
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Start update in {}", replicationPaths);
        StatusWithReleaseData status =
                callRemotePublicationReceiver("Starting update with " + replicationPaths,
                        httpClientContext, post, StatusWithReleaseData.class, null);
        if (status.updateInfo == null || StringUtils.isBlank(status.updateInfo.updateId)) { // impossible
            throw ExceptionUtil.logAndThrow(LOG,
                    new RemoteReplicationException(Message.error("Received no updateId"), null, status, null));
        }
        return status;
    }

    @Nonnull
    @Override
    public StatusWithReleaseData releaseInfo(@Nonnull ReplicationPaths replicationPaths)
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        List<NameValuePair> form = new ArrayList<>();
        replicationPaths.addToForm(form);
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(releaseInfo, json, replicationPaths.getContentPath());
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Get releaseinfo for {}", replicationPaths);
        }
        StatusWithReleaseData status =
                callRemotePublicationReceiver("Get releaseinfo for " + replicationPaths,
                        httpClientContext, post, StatusWithReleaseData.class, null);
        return status;
    }

    @Nonnull
    protected HttpClientContext createHttpClientContext() throws ReplicationException {
        HttpClientContext httpClientContext;
        try {
            httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                    proxyManagerService, credentialService);
        } catch (RepositoryException e) {
            throw new ReplicationException(Message.error("Trouble initializing connection for {}", replicationConfig.getPath()), e);
        }
        return httpClientContext;
    }

    @Override
    @Nonnull
    public ContentStateStatus contentState(
            @Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths, @Nonnull ResourceResolver resolver, @Nonnull ReplicationPaths replicationPaths)
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
        replicationPaths.addToForm(form);
        for (String path : paths) {
            form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_PATH, path));
        }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = uriString(contentState, json);
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        LOG.info("Querying content state for {} , {}", updateInfo.updateId, paths);
        Gson gson = new GsonBuilder().registerTypeAdapterFactory(
                new VersionableTree.VersionableTreeDeserializer(null, resolver, replicationPaths.getOrigin())
        ).create();
        ContentStateStatus status =
                callRemotePublicationReceiver("Querying content for " + paths,
                        httpClientContext, post, ContentStateStatus.class, gson);
        return status;
    }

    @Override
    @Nonnull
    public Status compareContent(@Nonnull UpdateInfo updateInfo, @Nonnull Collection<String> paths,
                                 ResourceResolver resolver, ReplicationPaths replicationPaths)
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        URIBuilder uriBuilder = uriBuilder(compareContent, json, replicationPaths.getContentPath())
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId);
        replicationPaths.addToUriBuilder(uriBuilder);
        URI uri = buildUrl(uriBuilder);
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
    public Status pathupload(@Nonnull UpdateInfo updateInfo, @Nonnull Resource resource) throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        URI uri = buildUrl(uriBuilder(pathUpload, zip, resource.getPath())
                .addParameter(RemoteReceiverConstants.PARAM_UPDATEID, updateInfo.updateId));
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
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
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
    public Status abortUpdate(@Nonnull UpdateInfo updateInfo) throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
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
    public Status compareParents(@Nonnull ReplicationPaths replicationPaths, @Nonnull ResourceResolver resolver,
                                 @Nonnull Stream<ChildrenOrderInfo> relevantOrderings,
                                 @Nonnull Stream<NodeAttributeComparisonInfo> attributeInfos)
            throws ReplicationException {
        HttpClientContext httpClientContext = createHttpClientContext();
        Gson gson = new GsonBuilder().create();

        HttpEntity entity = new JsonHttpEntity(null, null) {
            @Override
            protected void writeTo(@Nonnull JsonWriter jsonWriter) throws IOException {
                jsonWriter.beginObject();
                jsonWriter.name(RemoteReceiverConstants.PARAM_REPLICATIONPATHS);
                gson.toJson(replicationPaths, replicationPaths.getClass(), jsonWriter);

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

        String uri = uriString(compareParents, json);
        HttpPut put = new HttpPut(uri);
        put.setEntity(entity);

        LOG.info("Comparing parents for {}", replicationPaths);
        Status status = callRemotePublicationReceiver("Comparing parents for " + replicationPaths,
                httpClientContext, put, Status.class, null);
        return status;
    }

    @Nonnull
    protected <T extends Status> T callRemotePublicationReceiver(
            @Nonnull String logmessage, @Nonnull HttpClientContext httpClientContext, @Nonnull HttpUriRequest request,
            @Nonnull Class<T> statusClass, @Nullable Gson gson) throws ReplicationException {
        LOG.debug("Executing request {}", request.getURI());
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
                        new RemoteReplicationException(Message.error("Received invalid status from remote system for {}", logmessage), null, status, statusLine));
            }
        } catch (IOException e) {
            throw ExceptionUtil.logAndThrow(LOG,
                    new RemoteReplicationException(Message.error("Trouble accessing remote service for {}", logmessage), e, status, statusLine));
        }
        return status;
    }

}
