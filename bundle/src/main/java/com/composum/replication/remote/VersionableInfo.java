package com.composum.replication.remote;

import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.composum.sling.platform.staging.StagingConstants.PROP_REPLICATED_VERSION;

public class VersionableInfo {

    private static final Logger LOG = LoggerFactory.getLogger(VersionableInfo.class);

    private String path;
    private String version;

    /** Constructor for JSON deserialization. */
    public VersionableInfo() {
        // empty
    }

    /** Constructor when reading from content. */
    public VersionableInfo(Resource resource) {
        this.path = resource.getPath();
        this.version = resource.getValueMap().get(StagingConstants.PROP_REPLICATED_VERSION, String.class);
    }

    public String getPath() {
        return path;
    }

    public String getVersion() {
        return version;
    }

    @Nullable
    public static VersionableInfo of(@Nonnull Resource resource) {
        if (ResourceUtil.isNodeType(resource, ResourceUtil.TYPE_VERSIONABLE)) {
            String version = resource.getValueMap().get(StagingConstants.PROP_REPLICATED_VERSION, String.class);
            if (StringUtils.isNotBlank(version)) {
                VersionableInfo info = new VersionableInfo();
                info.path = resource.getPath();
                info.version = version;
                return info;
            } else { // that shouldn't happen in the intended usecase.
                LOG.warn("Something's wrong here: {} has no {}", resource.getPath(), PROP_REPLICATED_VERSION);
            }
        } else if (ResourceUtil.CONTENT_NODE.equals(resource.getName())) {
            // that shouldn't happen in the intended usecase.
            LOG.warn("Something's wrong here: {} has no {}", resource.getPath(), PROP_REPLICATED_VERSION);
        }
        return null;
    }

}
