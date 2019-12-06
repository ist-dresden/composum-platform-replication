package com.composum.replication.remote;

import com.composum.sling.core.AbstractSlingBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/** Bean modeling a remote publication configuration - subnode below /conf/{sitepath}/{site}/replication/ . */
public class RemotePublicationConfig extends AbstractSlingBean {

    /** Property name for {@link #getEnabled()}. */
    public static final String PROP_ENABLED = "enabled";
    /** Property name for {@link #getRelPath()}. */
    public static final String PROP_REL_PATH = "relPath";
    /** Property name for {@link #getUrl()}. */
    public static final String PROP_URL = "receiverUrl";
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
    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationConfig.class);
    /** @see #getEnabled() */
    private transient Boolean enabled;
    /** @see #getRelPath() */
    private transient String relPath;
    /** @see #getUrl() */
    private transient String receiverUri;
    /** @see #getUser() */
    private transient String user;
    /** @see #getPasswd() */
    private transient String passwd;
    /** @see #getProxyUser() */
    private transient String proxyUser;
    /** @see #getPassword() */
    private transient String password;
    /** @see #getProxyHost() */
    private transient String proxyHost;
    /** @see #getProxyPort() */
    private transient Integer proxyPort;
    /** @see #getProxyPassword() */
    private transient String proxyPassword;

    /** Whether this replication is enabled - default true. */
    public boolean getEnabled() {
        if (enabled == null) {
            enabled = getProperty(PROP_ENABLED, Boolean.TRUE);
        }
        return enabled;
    }

    /** Relative path wrt. to the site that should be replicated. If omitted, we take "." as the site root. */
    public String getRelPath() {
        if (relPath == null) {
            relPath = getProperty(PROP_REL_PATH, ".");
        }
        return relPath;
    }

    /** URL of the {@link RemotePublicationReceiverServlet} on the remote system. */
    public URI getReceiverUri() {
        if (receiverUri == null) {
            receiverUri = getProperty(PROP_URL, "");
        }
        try {
            return receiverUri != null ? new URI(receiverUri) : null;
        } catch (URISyntaxException e) {
            LOG.error("Broken URI {} at {}", receiverUri, getPath(), e);
            throw new IllegalStateException("Broken URI at configuration {}" + getPath(), e);
        }
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder
                .append("path", getPath())
                .append("relPath", relPath)
                .append("receiverUrl", receiverUri)
        ;
        if (!getEnabled()) { builder.append("enabled", enabled); }
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
        return password;
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

        URI targetHost = getReceiverUri();
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
                    new UsernamePasswordCredentials(getUser(), decodePassword(getProxyPassword(), passwordDecryptor)));
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
