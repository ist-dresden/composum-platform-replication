package com.composum.platform.replication.remote;

import com.composum.sling.platform.staging.replication.ReplicationType;

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
    public String getResourceType() {
        return "composum/platform/replication/remote";
    }
}
