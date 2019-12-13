package com.composum.platform.replication.remotereceiver;

/** Constants related to the remote receiver for receiving content from an author host at the publisher. */
public interface RemoteReceiverConstants {

    /** Parameter for a changed path. */
    String PARAM_PATH = "path";

    /** Parameter for a changed path. */
    String PARAM_DELETED_PATH = "deletedpath";

    /**
     * Mandatory parameter that points to the release root. Should be deliberately used as last part in the request,
     * to easily ensure that the whole request was transmitted.
     */
    String PARAM_RELEASEROOT = "releaseRoot";

    /** Attribute at the publishers temporary location that saves the top content path to be replaced. */
    String ATTR_CONTENTPATH = "contentPath";

    /** Attribute at the publishers temporary location that saves the original release change id of the publishers
     * content path - that can be checked to discover concurrent modifications. */
    String ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID = "originalPublisherReleaseChangeId";

    /** Attribute at the publishers temporary location that saves the release Root to write into. */
    String ATTR_RELEASEROOT_PATH = "releaseRoot";
}
