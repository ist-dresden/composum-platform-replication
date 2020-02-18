package com.composum.platform.replication.remote;

import com.composum.platform.replication.ReplicationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RemoteReplicationType implements ReplicationType {

    public static final String SERVICE_ID = "remote";

    @Nonnull
    @Override
    public String getServiceId() {
        return SERVICE_ID;
    }

    @Nonnull
    @Override
    public String getTitle() {
        return "Standard Remote";
    }

    @Nullable
    @Override
    public String getDescription() {
        return "the default remote replication implementation";
    }

    @Nonnull
    @Override
    public String getConfigResourceType() {
        return "composum/platform/replication/config/remote";
    }

    @Nonnull
    @Override
    public String getStatusResourceType() {
        return "composum/platform/replication/status/remote";
    }
}
