package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.credentials.CredentialService;
import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.replication.AbstractReplicationConfig;
import com.composum.sling.platform.staging.replication.ReplicationType;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Bean modeling a remote publication configuration - subnode below /conf/{sitepath}/{site}/replication/ .
 */
public class RemotePublicationConfig extends AbstractReplicationConfig {
    public static final ReplicationType REMOTE_REPLICATION_TYPE = new RemoteReplicationType();
    /**
     * Property name for {@link #getCredentialId()}.
     */
    public static final String PROP_CREDENTIAL_ID = "credentialId";

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationConfig.class);

    protected String targetUrl;
    protected String proxyKey;
    protected String credentialId;

    @Override
    public void initialize(BeanContext context, Resource resource) {
        super.initialize(context, resource);
        this.targetUrl = getProperty(PROP_URL, "");
        this.proxyKey = getProperty(PROP_PROXY_KEY, String.class);
        this.credentialId = getProperty(PROP_CREDENTIAL_ID, "");
    }

    /**
     * URL of the {@link RemotePublicationReceiverServlet} on the remote system.
     */
    public URI getTargetUrl() {
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

    @Nonnull
    @Override
    public ReplicationType getReplicationType() {
        return REMOTE_REPLICATION_TYPE;
    }

    /**
     * Optionally, the key of the proxy we need to use to reach the remote system.
     */
    public String getProxyKey() {
        return proxyKey;
    }

    /**
     * Optional ID to retrieve the credentials from the {@link com.composum.platform.commons.credentials.CredentialService}.
     */
    public String getCredentialId() {
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
