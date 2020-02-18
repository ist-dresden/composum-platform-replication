package com.composum.platform.replication.model;

import com.composum.platform.replication.inplace.InplaceReplicationType;
import com.composum.platform.replication.ReplicationType;
import com.composum.platform.replication.remote.RemoteReplicationType;
import com.composum.sling.core.AbstractSlingBean;
import com.composum.sling.core.util.ResourceUtil;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.composum.platform.replication.ReplicationConstants.RT_REPLICATION_SETUP;

public class ReplicationSetup extends AbstractSlingBean {

    private transient Map<String, ReplicationType> replicationTypes;

    private transient Map<String, List<ReplicationConfig>> setupByPath;
    private transient Map<String, List<ReplicationConfig>> setupByType;

    private transient List<ReplicationConfig> setup;

    public class ConfigNode implements ReplicationConfig {

        protected final Resource node;
        protected final ValueMap values;

        public ConfigNode(Resource node) {
            this.node = node;
            this.values = node.getValueMap();
        }

        @Nonnull
        @Override
        public String getTitle() {
            return values.get(ResourceUtil.JCR_TITLE, node.getName());
        }

        @Nullable
        @Override
        public String getDescription() {
            return values.get(ResourceUtil.JCR_DESCRIPTION, String.class);
        }

        @Nonnull
        @Override
        public String getPath() {
            return node.getPath();
        }

        @Nonnull
        @Override
        public String getContentPath() {
            return values.get(PN_CONTENT_PATH, "");
        }

        @Nonnull
        @Override
        public ReplicationType getReplicationType() {
            return getReplicationTypes().get(values.get(PN_REPLICATIN_TYPE, InplaceReplicationType.SERVICE_ID));
        }

        @Override
        public boolean isEditable() {
            return values.get(PN_IS_EDITABLE, Boolean.FALSE);
        }
    }

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
        if (setup == null) {
            setup = new ArrayList<>();
            if (resource.isResourceType(RT_REPLICATION_SETUP)) {
                for (Resource node : resource.getChildren()) {
                    setup.add(new ConfigNode(node));
                }
            }
        }
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
