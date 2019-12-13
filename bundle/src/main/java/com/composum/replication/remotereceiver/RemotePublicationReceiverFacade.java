package com.composum.replication.remotereceiver;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.replication.remote.RemotePublisherService;
import com.composum.replication.remotereceiver.RemotePublicationReceiverServlet.Extension;
import com.composum.replication.remotereceiver.RemotePublicationReceiverServlet.Operation;
import com.composum.sling.core.servlet.Status;
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
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.sling.api.resource.Resource;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/** Provides a Java interface for accessing the remote publication receiver. */
public class RemotePublicationReceiverFacade {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverFacade.class);

    @Nonnull
    protected final RemotePublicationConfig replicationConfig;

    @Nonnull
    protected final CryptoService cryptoService;

    @Nonnull
    protected final Supplier<RemotePublisherService.Configuration> generalConfig;

    @Nonnull
    protected final CloseableHttpClient httpClient;

    public RemotePublicationReceiverFacade(@Nonnull RemotePublicationConfig replicationConfig,
                                           @Nonnull CryptoService cryptoService,
                                           @Nonnull CloseableHttpClient httpClient,
                                           @Nonnull Supplier<RemotePublisherService.Configuration> generalConfig) {
        this.replicationConfig = Objects.requireNonNull(replicationConfig);
        this.cryptoService = cryptoService;
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
     *
     * @param releaseRoot the root of the release containing {path} (may be equal to {path})
     * @param path the root content path that should be considered. Might be the root of a release, or any
     *             subdirectory.
     * @return the basic information about the update which must be used for all related calls on this update.
     */
    @Nonnull
    public StartUpdateOperation.UpdateInfo startUpdate(@NotNull String releaseRoot, @Nonnull String path) throws RemotePublicationReceiverException {
        HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                passwordDecryptor());
        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair(RemoteReceiverConstants.PARAM_RELEASEROOT, releaseRoot));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);
        String uri = replicationConfig.getReceiverUri() + "." + Operation.startupdate.name() + "." + Extension.json.name() + path;
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);

        Class<StartUpdateOperation.StatusWithReleaseData> statusClass = StartUpdateOperation.StatusWithReleaseData.class;
        StartUpdateOperation.StatusWithReleaseData status = callRemotePublicationReceiver(path, httpClientContext, post, statusClass);
        StartUpdateOperation.UpdateInfo updateInfo = status.updateInfo;
        return updateInfo;
    }

    @Nonnull
    protected StartUpdateOperation.StatusWithReleaseData callRemotePublicationReceiver(@Nonnull String path, HttpClientContext httpClientContext, HttpUriRequest request, Class<StartUpdateOperation.StatusWithReleaseData> statusClass) throws RemotePublicationReceiverException {
        StartUpdateOperation.StatusWithReleaseData status = null;
        StatusLine statusLine = null;
        try (CloseableHttpResponse response = httpClient.execute(request, httpClientContext)) {
            statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream content = entity.getContent()) {
                    Charset charset;
                    Reader contentReader = new InputStreamReader(content, StandardCharsets.UTF_8);
                    Gson gson = new GsonBuilder().create();
                    status = gson.fromJson(contentReader, statusClass);
                }
            }
            if (status != null && status.isValid() && status.isSuccess()) {
                LOG.info("Remote replication successful with {}, {}", statusLine.getStatusCode(), statusLine.getReasonPhrase());
            } else {
                throw new RemotePublicationReceiverException("Received invalid status from remote system", null,
                        status, statusLine);
            }
        } catch (IOException e) {
            LOG.error("", e);
            throw new RemotePublicationReceiverException("Trouble accessing remote service for " + path, e, status,
                    statusLine);
        }
        return status;
    }

    /** Uploads the resource tree to the remote machine. */
    public void pathupload(@Nonnull StartUpdateOperation.UpdateInfo updateInfo, @Nonnull Resource resource) throws RemotePublicationReceiverException {
        throw new UnsupportedOperationException("Not implemented yet."); // FIXME hps 11.12.19 not implemented
    }

    /** Executes the update. */
    public void commitUpdate(@Nonnull StartUpdateOperation.UpdateInfo updateInfo) throws RemotePublicationReceiverException {
        throw new UnsupportedOperationException("Not implemented yet."); // FIXME hps 11.12.19 not implemented
    }

    /** Aborts the update, deleting the temporary directory on the remote side. */
    public void abortUpdate(@Nonnull StartUpdateOperation.UpdateInfo updateInfo) throws RemotePublicationReceiverException {
        throw new UnsupportedOperationException("Not implemented yet."); // FIXME hps 11.12.19 not implemented
    }

    /** Exception that signifies a problem with the replication. */
    public static class RemotePublicationReceiverException extends Exception {
        protected final Status status;
        protected final Integer statusCode;
        protected final String reasonPhrase;

        public RemotePublicationReceiverException(String message, Throwable throwable, Status status, StatusLine statusLine) {
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
