package com.composum.platform.replication.model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReplicationConfig {

    @Nonnull
    String getTitle();

    @Nullable
    String getDescription();

    @Nonnull
    String getContentPath();

    @Nonnull
    ReplicationType getReplicationType();
    
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
