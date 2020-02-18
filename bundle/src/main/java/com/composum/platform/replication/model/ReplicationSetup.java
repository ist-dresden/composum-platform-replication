package com.composum.platform.replication.model;

import com.composum.platform.replication.ReplicationType;
import com.composum.platform.replication.inplace.InplaceReplicationType;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.AbstractSlingBean;
import com.composum.sling.cpnl.CpnlElFunctions;
import org.apache.sling.api.resource.Resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.composum.platform.replication.ReplicationConstants.RT_REPLICATION_SETUP;

public class ReplicationSetup extends AbstractSlingBean {

    public static final Pattern NO_IDENTIFIER_CHAR = Pattern.compile("[^a-zA-Z0-9_-]+");

    private transient Map<String, ReplicationType> replicationTypes;

    public class ConfigSet {

        protected final String key;
        protected final List<ReplicationConfig> set = new ArrayList<>();

        public ConfigSet(String key) {
            this.key = CpnlElFunctions.text(key);
        }

        public String getId() {
            return NO_IDENTIFIER_CHAR.matcher(getKey()
                    .replace('/', '-'))
                    .replaceAll("");
        }

        public String getKey() {
            return key;
        }

        public String getTitle() {
            return getKey();
        }

        public List<ReplicationConfig> getSet() {
            return set;
        }

        @Override
        public String toString() {
            return getKey();
        }
    }

    private transient Map<String, ConfigSet> setupByPath;
    private transient Map<String, ConfigSet> setupByType;

    private transient List<ReplicationConfig> setup;

    public Map<String, ReplicationType> getReplicationTypes() {
        // ToDo scan available services...
        if (replicationTypes == null) {
            replicationTypes = new LinkedHashMap<>() {{
                put(InplaceReplicationType.SERVICE_ID, new InplaceReplicationType());
                put(RemoteReplicationType.SERVICE_ID, new RemoteReplicationType());
            }};
        }
        return replicationTypes;
    }

    public Collection<ConfigSet> getSetupByPath() {
        if (setupByPath == null) {
            setupByPath = getGrouped(new ReplicationConfig.PathComparator());
        }
        return setupByPath.values();
    }

    public Collection<ConfigSet> getSetupByType() {
        if (setupByType == null) {
            setupByType = getGrouped(new ReplicationConfig.TypeComparator());
        }
        return setupByType.values();
    }

    /**
     * @return the set of replication configurations for the models resource (release owner)
     */
    protected List<ReplicationConfig> getSetup() {
        // ToDo scan configuration...
        if (setup == null) {
            setup = new ArrayList<>();
            if (resource.isResourceType(RT_REPLICATION_SETUP)) {
                for (Resource node : resource.getChildren()) {
                    setup.add(new ReplicationConfigNode(context, node));
                }
            }
        }
        return setup;
    }

    protected Map<String, ConfigSet> getGrouped(ReplicationConfig.Comparator comparator) {
        Map<String, ConfigSet> result = new LinkedHashMap<>();
        List<ReplicationConfig> setup = getSetup();
        setup.sort(comparator);
        for (ReplicationConfig config : setup) {
            String key = comparator.getKey(config);
            result.computeIfAbsent(key, k -> new ConfigSet(key)).getSet().add(config);
        }
        return result;
    }
}
