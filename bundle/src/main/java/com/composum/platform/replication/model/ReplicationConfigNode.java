package com.composum.platform.replication.model;

import com.composum.platform.commons.proxy.ProxyManagerService;
import com.composum.platform.commons.proxy.ProxyService;
import com.composum.platform.replication.ReplicationType;
import com.composum.platform.replication.inplace.InplaceReplicationType;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.ResourceModel;
import com.composum.sling.core.util.I18N;
import com.composum.sling.core.util.ResourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReplicationConfigNode extends ResourceModel implements ReplicationConfig {

    private transient String proxyOptions;

    public ReplicationConfigNode() {
    }

    public ReplicationConfigNode(BeanContext context, Resource node) {
        super(context, node);
    }

    @Nonnull
    @Override
    public String getStage() {
        return getProperty(PN_STAGE, "");
    }

    @Nonnull
    @Override
    public String getTitle() {
        return getProperty(ResourceUtil.JCR_TITLE, getResource().getName());
    }

    @Nullable
    @Override
    public String getDescription() {
        return getProperty(ResourceUtil.JCR_DESCRIPTION, String.class);
    }

    @Nonnull
    @Override
    public String getSourcePath() {
        String path = getProperty(PN_SOURCE_PATH, "");
        if (StringUtils.isBlank(path)) {
            path = getSitePath();
        }
        return path;
    }

    @Nonnull
    public String getSitePath() {
        return getPath().replaceAll("/conf(/.*)/replication/[^/]+", "$1");
    }

    @Nullable
    @Override
    public String getTargetPath() {
        return getProperty(PN_TARGET_PATH, String.class);
    }

    @Nonnull
    @Override
    public ReplicationType getReplicationType() {
        // ToDo retrieve replication type
        String key = getProperty(PN_REPLICATIN_TYPE, InplaceReplicationType.SERVICE_ID);
        return InplaceReplicationType.SERVICE_ID.equals(key)
                ? new InplaceReplicationType() : new RemoteReplicationType();
    }

    @Override
    public boolean isEnabled() {
        return getProperty(PN_IS_ENABLED, Boolean.FALSE);
    }

    @Override
    public boolean isEditable() {
        return getProperty(PN_IS_EDITABLE, Boolean.FALSE);
    }

    @Nonnull
    @Override
    public String getConfigResourceType() {
        String resourceType = getProperty(ResourceUtil.PROP_RESOURCE_TYPE, "");
        return StringUtils.isBlank(resourceType) ? getReplicationType().getResourceType() : resourceType;
    }

    public String getProxyOptions() {
        if (proxyOptions == null) {
            StringBuilder result = new StringBuilder(":" + I18N.get(context.getRequest(), "no proxy"));
            ProxyManagerService proxyManager = context.getService(ProxyManagerService.class);
            for (String key : proxyManager.getProxyKeys()) {
                ProxyService proxy = proxyManager.findProxyService(key);
                if (proxy != null) {
                    String title = proxy.getTitle();
                    result.append(',').append(key);
                    if (StringUtils.isNotBlank(title)) {
                        result.append(':').append(title);
                    }
                }
            }
            proxyOptions = result.toString();
        }
        return proxyOptions;
    }
}
