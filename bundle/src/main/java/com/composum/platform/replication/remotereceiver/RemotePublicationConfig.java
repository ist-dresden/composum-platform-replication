package com.composum.platform.replication.remotereceiver;

import com.composum.platform.replication.ReplicationType;
import com.composum.platform.replication.model.ReplicationConfig;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.AbstractSlingBean;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.security.AccessMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.sling.jcr.resource.api.JcrResourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/** Bean modeling a remote publication configuration - subnode below /conf/{sitepath}/{site}/replication/ . */
public class RemotePublicationConfig extends AbstractSlingBean implements ReplicationConfig {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationConfig.class);

    /** Property name for {@link #isEnabled()}. */
    public static final String PROP_ENABLED = "enabled";
    /** Property name for {@link #getUrl()}. */
    public static final String PROP_URL = "targetUrl";
    /** Property name for {@link #getUser()}. */
    public static final String PROP_USER = "user";
    /** Property name for {@link #getPasswd()}. */
    public static final String PROP_PASSWD = "passwd";
    /** Property name for {@link #getProxyUser()}. */
    public static final String PROP_PROXY_USER = "proxyUser";
    /** Property name for {@link #getProxyPassword()}. */
    public static final String PROP_PROXY_PASSWORD = "proxyPassword";
    /** Property name for {@link #getProxyHost()}. */
    public static final String PROP_PROXY_HOST = "proxyHost";
    /** Property name for {@link #getProxyPort()}. */
    public static final String PROP_PROXY_PORT = "proxyPort";
    /** Property name for {@link #getStage()}. */
    public static final String PROP_ACCESS_MODE = "stage";
    /** Property name for {@link #getSourcePath()}. */
    public static final String PROP_SOURCE_PATH = "sourcePath";
    /** Property name for {@link #getTargetPath()}. */
    public static final String PROP_TARGET_PATH = "targetPath";
    /** Property name for {@link #getProxyKey()}. */
    public static final String PROP_PROXY_KEY = "proxyKey";

    protected static final ReplicationType REMOTE_REPLICATION_TYPE = new RemoteReplicationType();

    /** @see #isEnabled() */
    private transient Boolean enabled;
    /** @see #getUrl() */
    private transient String targetUrl;
    /** @see #getUser() */
    private transient String user;
    /** @see #getPasswd() */
    private transient String passwd;
    /** @see #getProxyUser() */
    private transient String proxyUser;
    /** @see #getProxyHost() */
    private transient String proxyHost;
    /** @see #getProxyPort() */
    private transient Integer proxyPort;
    /** @see #getProxyPassword() */
    private transient String proxyPassword;
    /** @see #getStage() */
    private transient String stage;
    /** @see #getSourcePath() */
    private transient String sourcePath;
    /** @see #getTargetPath() */
    private transient String targetPath;
    /** @see #getProxyKey() */
    private transient String proxyKey;
    /** @see #getDescription() */
    private transient String description;

    /** Optional human-readable description. */
    @Override
    public String getDescription() {
        if (description == null) {
            description = getProperty(ResourceUtil.PROP_DESCRIPTION, String.class);
        }
        return description;
    }

    /**
     * The release mark (mostly {@value com.composum.sling.platform.security.AccessMode#PUBLIC} /
     * {@value com.composum.sling.platform.security.AccessMode#PREVIEW}) for which the release is replicated.
     * If empty, there is no replication.
     */
    @Override
    public String getStage() {
        if (stage == null) {
            stage = getProperty(PROP_ACCESS_MODE, AccessMode.PUBLIC.name());
        }
        return stage;
    }

    /** Whether this replication is enabled - default true. */
    @Override
    public boolean isEnabled() {
        if (enabled == null) {
            enabled = getProperty(PROP_ENABLED, Boolean.TRUE);
        }
        return enabled;
    }

    @Override
    public boolean isEditable() {
        return true;
    }

    /** URL of the {@link RemotePublicationReceiverServlet} on the remote system. */
    public URI getTargetUrl() {
        if (targetUrl == null) {
            targetUrl = getProperty(PROP_URL, "");
        }
        try {
            return targetUrl != null ? new URI(targetUrl) : null;
        } catch (URISyntaxException e) {
            LOG.error("Broken URI {} at {}", targetUrl, getPath(), e);
            throw new IllegalStateException("Broken URI at configuration {}" + getPath(), e);
        }
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder
                .append("stage", getStage())
                .append("path", getPath())
                .append("targetUrl", getTargetUrl())
                .append("enabled", isEnabled())
        ;
        return builder.toString();
    }

    /** Optional username for authentication with the receiver. */
    public String getUser() {
        if (user == null) {
            user = getProperty(PROP_USER, "");
        }
        return user;
    }

    /** Optional password for authentication with the receiver. */
    public String getPasswd() {
        if (passwd == null) {
            passwd = getProperty(PROP_PASSWD, "");
        }
        return passwd;
    }

    /** Optional user for authentication with the proxy. */
    public String getProxyUser() {
        if (proxyUser == null) {
            proxyUser = getProperty(PROP_PROXY_USER, "");
        }
        return proxyUser;
    }

    /** Optional password for the authentication with the proxy. */
    public String getProxyPassword() {
        if (proxyPassword == null) {
            proxyPassword = getProperty(PROP_PROXY_PASSWORD, "");
        }
        return proxyPassword;
    }

    /** Optionally, the host of a proxy. */
    public String getProxyHost() {
        if (proxyHost == null) {
            proxyHost = getProperty(PROP_PROXY_HOST, "");
        }
        return proxyHost;
    }

    /** Optionally, the port of a proxy. */
    public Integer getProxyPort() {
        if (proxyPort == null) {
            proxyPort = getProperty(PROP_PROXY_PORT, Integer.class);
        }
        return proxyPort;
    }

    /** Optional, the path we replicate - must be the site or a subpath of the site. */
    @Override
    public String getSourcePath() {
        if (sourcePath == null) {
            sourcePath = getProperty(PROP_SOURCE_PATH, String.class);
        }
        return sourcePath;
    }

    /** Optional, the path we replicate to. If not given, this is equivalent to the source Path. */
    @Override
    public String getTargetPath() {
        if (targetPath == null) {
            targetPath = getProperty(PROP_TARGET_PATH, String.class);
        }
        return targetPath;
    }

    @Nonnull
    @Override
    public ReplicationType getReplicationType() {
        return REMOTE_REPLICATION_TYPE;
    }

    @Nonnull
    @Override
    public String getConfigResourceType() {
        return getProperty(JcrResourceConstants.SLING_RESOURCE_TYPE_PROPERTY, String.class);
    }

    /** Optionally, the key of the proxy we need to use to reach the remote system. */
    public String getProxyKey() {
        if (proxyKey == null) {
            proxyKey = getProperty(PROP_PROXY_KEY, String.class);
        }
        return proxyKey;
    }

    /**
     * Initializes a HttpClientContext for httpclient with the saved data (auth, proxy).
     *
     * @param context           the context to initialize
     * @param passwordDecryptor if given, the password is piped through that to deobfuscate it
     * @return context
     */
    @Nonnull
    public HttpClientContext initHttpContext(@Nonnull HttpClientContext context,
                                             @Nullable Function<String, String> passwordDecryptor) {
        CredentialsProvider credsProvider = context.getCredentialsProvider() != null ?
                context.getCredentialsProvider() : new BasicCredentialsProvider();
        boolean needCredsProvider = false;

        URI targetHost = getTargetUrl();
        if (isNotBlank(getUser())) {
            credsProvider.setCredentials(
                    new AuthScope(targetHost.getHost(), targetHost.getPort()),
                    new UsernamePasswordCredentials(getUser(), decodePassword(getPasswd(), passwordDecryptor)));
            needCredsProvider = true;
        }


        if (isNotBlank(getProxyHost()) && isNotBlank(getProxyUser())) {
            needCredsProvider = true;
            credsProvider.setCredentials(
                    new AuthScope(getProxyHost(), getProxyPort()),
                    new UsernamePasswordCredentials(getProxyUser(), decodePassword(getProxyPassword(), passwordDecryptor)));
        }
        if (needCredsProvider) { context.setCredentialsProvider(credsProvider);}

        if (isNotBlank(getProxyHost())) {
            RequestConfig.Builder requestConfigBuilder = context.getRequestConfig() != null ?
                    RequestConfig.copy(context.getRequestConfig()) : RequestConfig.custom();
            HttpHost proxy = new HttpHost(getProxyHost(), getProxyPort());
            requestConfigBuilder.setProxy(proxy);
        }
        return context;
    }

    protected String decodePassword(String password, Function<String, String> passwordDecryptor) {
        String result = password;
        if (StringUtils.isNotBlank(password) && passwordDecryptor != null) {
            result = passwordDecryptor.apply(password);
        }
        return result;
    }
}
