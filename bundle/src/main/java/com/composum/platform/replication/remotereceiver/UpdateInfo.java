package com.composum.platform.replication.remotereceiver;

/** Class with general information about the running update. */
public class UpdateInfo {

    /**
     * The update id for the pending operation - has to be named like
     * {@link AbstractContentUpdateOperation#PARAM_UPDATEID}.
     */
    public String updateId;

    /** The original status of the publishers release. */
    public String originalPublisherReleaseChangeId;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UpdateInfo{");
        sb.append("updateId='").append(updateId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
