package com.composum.platform.replication.model;

import com.composum.platform.replication.ReplicationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReplicationConfig {

    String PN_CONTENT_PATH = "contentPath";
    String PN_REPLICATIN_TYPE = "replicationType";
    String PN_IS_ENABLED = "enabled";
    String PN_IS_EDITABLE = "editable";

    @Nonnull
    String getTitle();

    @Nullable
    String getDescription();

    /**
     * @return the path of the configuration resource itself
     */
    String getPath();

    /**
     * @return the path of the content affected by this configuration
     */
    @Nonnull
    String getContentPath();

    /**
     * @return the replication service type (implementation type)
     */
    @Nonnull
    ReplicationType getReplicationType();

    /**
     * @return the resource type of the component to view / edit the configuration
     */
    @Nonnull
    String getConfigResourceType();

    /**
     * @return 'true' if the replication declared by this configuration is enabled
     */
    boolean isEnabled();

    /**
     * @return 'true' if the configuration can be changed by the user
     */
    boolean isEditable();
}
