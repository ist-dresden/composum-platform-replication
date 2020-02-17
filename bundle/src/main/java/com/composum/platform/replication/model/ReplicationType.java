package com.composum.platform.replication.model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReplicationType {

    @Nonnull
    String getServiceId();

    @Nonnull
    String getTitle();

    @Nullable
    String getDescription();

    @Nonnull
    String getResourceType();
}
