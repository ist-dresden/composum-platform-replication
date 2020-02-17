package com.composum.platform.replication.model;

import com.composum.sling.core.AbstractSlingBean;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ReplicationSetup extends AbstractSlingBean {

    private transient List<ReplicationType> replicationTypes;

    private transient Map<String, List<ReplicationConfig>> setupByPath;
    private transient Map<String, List<ReplicationConfig>> setupByType;

    private transient List<ReplicationConfig> setup;

    public List<ReplicationType> getReplicationTypes() {
        // ToDo scan available services...
        return replicationTypes;
    }

    public Map<String, List<ReplicationConfig>> getSetupByPath() {
        if (setupByPath == null) {
            setupByPath = getGrouped(new ReplicationConfig.PathComparator());
        }
        return setupByPath;
    }

    public Map<String, List<ReplicationConfig>> getSetupByType() {
        if (setupByType == null) {
            setupByType = getGrouped(new ReplicationConfig.TypeComparator());
        }
        return setupByType;
    }

    /**
     * @return the set of replication configurations for the models resource (release owner)
     */
    protected List<ReplicationConfig> getSetup() {
        // ToDo scan configuration...
        return setup;
    }

    protected Map<String, List<ReplicationConfig>> getGrouped(ReplicationConfig.Comparator comparator) {
        Map<String, List<ReplicationConfig>> result = new LinkedHashMap<>();
        List<ReplicationConfig> setup = getSetup();
        setup.sort(comparator);
        for (ReplicationConfig config : setup) {
            result.computeIfAbsent(comparator.getKey(config), k -> new ArrayList<>()).add(config);
        }
        return result;
    }
}
