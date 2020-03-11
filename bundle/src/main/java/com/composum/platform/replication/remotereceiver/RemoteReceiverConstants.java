package com.composum.platform.replication.remotereceiver;

import com.composum.sling.platform.staging.StagingReleaseManager;
import com.composum.sling.platform.staging.replication.ReplicationConstants;

/**
 * Constants related to the remote receiver for receiving content from an author host at the publisher.
 */
public interface RemoteReceiverConstants extends ReplicationConstants {

    /**
     * Parameter for a deleted path.
     */
    String PARAM_DELETED_PATH = "deletedpath";

    /**
     * Parameter for a {@link StagingReleaseManager.Release#getChangeNumber()}.
     */
    String PARAM_RELEASE_CHANGENUMBER = "releaseChangeNumber";

    /**
     * Mandatory of the parameter to contain the update id (except for startupdate).
     */
    String PARAM_UPDATEID = "updateId";

    /**
     * Attribute at the publishers temporary location that saves the top content path to be replaced.
     */
    String ATTR_TOP_CONTENTPATH = "topContentPath";

    /**
     * Attribute at the publishers temporary location that saves the original release change id of the publishers
     * content path - that can be checked to discover concurrent modifications.
     */
    String ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID = "originalPublisherReleaseChangeId";

    /**
     * Attribute at the publishers temporary location that saves the release Root to write into.
     */
    String ATTR_RELEASEROOT_PATH = "releaseRoot";

    /**
     * Attribute at the publishers temporary location that saves the release Root to write into.
     */
    String ATTR_SRCPATH = "sourcePath";

    /**
     * Attribute at the publishers temporary location that saves the release Root to write into.
     */
    String ATTR_TARGETPATH = "targetPath";

    /**
     * Attribute at the publishers temporary location that saves the paths which were uploaded by the publisher
     * and whose content needs to be moved into the main content.
     */
    String ATTR_UPDATEDPATHS = "updatedPaths";
}
