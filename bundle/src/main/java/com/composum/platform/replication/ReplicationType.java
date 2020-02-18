package com.composum.platform.replication;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReplicationType {

    /**
     * @return a unique key of the replication implementation
     */
    @Nonnull
    String getServiceId();

    /**
     * @return a short readable identifier of the replication implementation (i18n)
     */
    @Nonnull
    String getTitle();

    /**
     * @return a description of the replication implementation purpose and details (i18n)
     */
    @Nullable
    String getDescription();

    /**
     * @return the resource type of the replication implementation configuration component
     */
    @Nonnull
    String getConfigResourceType();

    /**
     * @return the resource type of the replication implementation status view component
     */
    @Nonnull
    String getStatusResourceType();
}
