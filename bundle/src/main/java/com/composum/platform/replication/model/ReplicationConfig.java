package com.composum.platform.replication.model;

import com.composum.platform.replication.ReplicationType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReplicationConfig {

    String PN_CONTENT_PATH = "contentPath";
    String PN_REPLICATIN_TYPE = "replicationType";
    String PN_IS_EDITABLE = "isEditable";

    @Nonnull
    String getTitle();

    @Nullable
    String getDescription();

    /**
     * @return the path of the configuration resource itself
     */
    @Nonnull
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
     * @return 'true' if the configuration can be changed by the user
     */
    boolean isEditable();

    //
    // to support grouping...
    //

    abstract class Comparator implements java.util.Comparator<ReplicationConfig> {

        public abstract String getKey(ReplicationConfig config);

        public abstract String getSortValue(ReplicationConfig config);

        @Override
        public int compare(ReplicationConfig o1, ReplicationConfig o2) {
            return getSortValue(o1).compareTo(getSortValue(o2));
        }
    }

    class PathComparator extends Comparator {

        @Override
        public String getKey(ReplicationConfig config) {
            return config.getContentPath();
        }

        @Override
        public String getSortValue(ReplicationConfig config) {
            return getKey(config) + "_@@@_" + config.getReplicationType().getTitle();
        }
    }

    class TypeComparator extends Comparator {

        @Override
        public String getKey(ReplicationConfig config) {
            return config.getReplicationType().getTitle();
        }

        @Override
        public String getSortValue(ReplicationConfig config) {
            return getKey(config) + "_@@@_" + config.getContentPath();
        }
    }
}
