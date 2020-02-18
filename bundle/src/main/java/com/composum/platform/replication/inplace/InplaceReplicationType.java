package com.composum.platform.replication.inplace;

import com.composum.platform.replication.ReplicationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InplaceReplicationType implements ReplicationType {

    public static final String SERVICE_ID = "inplace";

    @Nonnull
    @Override
    public String getServiceId() {
        return SERVICE_ID;
    }

    @Nonnull
    @Override
    public String getTitle() {
        return "In Place";
    }

    @Nullable
    @Override
    public String getDescription() {
        return "the 'on system' in place replication";
    }

    @Nonnull
    @Override
    public String getConfigResourceType() {
        return "composum/platform/replication/config/inplace";
    }
}
