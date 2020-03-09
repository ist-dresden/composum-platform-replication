package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.sling.platform.staging.replication.ReplicationType;
import com.composum.sling.platform.staging.replication.ReplicationConfig;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.AbstractSlingBean;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.security.AccessMode;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.protocol.HttpClientContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/** Bean modeling a remote publication configuration - subnode below /conf/{sitepath}/{site}/replication/ . */
public class RemotePublicationConfig extends AbstractSlingBean implements ReplicationConfig {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationConfig.class);

    /** Property name for {@link #getUrl()}. */
    public static final String PROP_URL = "targetUrl";
    /** Property name for {@link #getProxyKey()}. */
    public static final String PROP_PROXY_KEY = "proxyKey";

    protected static final ReplicationType REMOTE_REPLICATION_TYPE = new RemoteReplicationType();

    /** Optional human-readable description. */
    @Override
    public String getDescription() {
        return getProperty(ResourceUtil.PROP_DESCRIPTION, String.class);
    }

    /**
     * The release mark (mostly {@link com.composum.sling.platform.security.AccessMode#PUBLIC} /
     * {@link com.composum.sling.platform.security.AccessMode#PREVIEW}) for which the release is replicated.
     * If empty, there is no replication.
     */
    @Nonnull
    @Override
    public String getStage() {
        return getProperty(PN_STAGE, AccessMode.PUBLIC.name());
    }

    /** Whether this replication is enabled - default true. */
    @Override
    public boolean isEnabled() {
        return getProperty(PN_IS_ENABLED, Boolean.TRUE);
    }

    @Override
    public boolean isEditable() {
        return getProperty(PN_IS_EDITABLE, true);
    }

    /** URL of the {@link RemotePublicationReceiverServlet} on the remote system. */
    public URI getTargetUrl() {
        String targetUrl = getProperty(PROP_URL, "");
        try {
            return targetUrl != null ? new URI(targetUrl) : null;
        } catch (URISyntaxException e) {
            LOG.error("Broken URI {} at {}", targetUrl, getPath(), e);
            return null;
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


    /** Optional, the path we replicate - must be the site or a subpath of the site. */
    @Nonnull
    @Override
    public String getSourcePath() {
        return getProperty(PN_SOURCE_PATH, String.class);
    }

    /** Optional, the path we replicate to. If not given, this is equivalent to the source Path. */
    @Override
    public String getTargetPath() {
        return getProperty(PN_TARGET_PATH, String.class);
    }

    @Nonnull
    @Override
    public ReplicationType getReplicationType() {
        return REMOTE_REPLICATION_TYPE;
    }

    @Nonnull
    @Override
    public String getConfigResourceType() {
        return getProperty(ResourceUtil.PROP_RESOURCE_TYPE, String.class);
    }

    /** Optionally, the key of the proxy we need to use to reach the remote system. */
    public String getProxyKey() {
        return getProperty(PROP_PROXY_KEY, String.class);
    }

    /** Property name for {@link #getCredentialId()}. */
    public static final String PROP_CREDENTIAL_ID = "credentialId";

    /** @see #getCredentialId() */
    private transient String credentialId;

    /** Optional ID to retrieve the credentials from the {@link com.composum.platform.commons.credentials.CredentialService}. */
    public String getCredentialId() {
        if (credentialId == null) {
            credentialId = getProperty(PROP_CREDENTIAL_ID, "");
        }
        return credentialId;
    }

    /**
     * Initializes a HttpClientContext for httpclient with the saved data (auth, proxy).
     *
     * @param context the context to initialize
     * @return context
     */
    @Nonnull
    public HttpClientContext initHttpContext(@Nonnull HttpClientContext context,
                                             @Nonnull ProxyManagerService proxyManagerService,
                                             @Nonnull CredentialService credentialService) throws RepositoryException {

        if (isNotBlank(getCredentialId())) {
            URI targetHost = getTargetUrl();
            AuthScope authScope = new AuthScope(targetHost.getHost(), targetHost.getPort());
            credentialService.initHttpContextCredentials(context, authScope, getCredentialId(), resolver);
        }

        if (isNotBlank(getProxyKey())) {
            proxyManagerService.initHttpContext(getProxyKey(), context, resolver);
        }

        return context;
    }

}
