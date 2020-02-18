package com.composum.platform.replication.model;

import com.composum.platform.replication.ReplicationType;
import com.composum.platform.replication.inplace.InplaceReplicationType;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.ResourceModel;
import com.composum.sling.core.util.ResourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ReplicationConfigNode extends ResourceModel implements ReplicationConfig {

    public ReplicationConfigNode() {
    }

    public ReplicationConfigNode(BeanContext context, Resource node) {
        super(context, node);
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
    public String getContentPath() {
        String path = getProperty(PN_CONTENT_PATH, "");
        if (StringUtils.isBlank(path)) {
            path = getProperty("relPath", "");
            if (".".equals(path)) {
                path = "";
            }
            path = getPath().replaceAll("/conf(/.*)/replication/[^/]+", "$1")
                    + (StringUtils.isNotBlank(path) ? ("/" + path) : "");
        }
        return path;
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
        return StringUtils.isBlank(resourceType) ? getReplicationType().getConfigResourceType() : resourceType;
    }
}
