package com.composum.platform.replication.remotereceiver;

import com.composum.platform.replication.json.VersionableInfo;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.Consumer;

/** Interface for service that implements the functions behind the {@link RemotePublicationReceiverServlet}. */
public interface RemotePublicationReceiver {

    boolean isEnabled();

    /** @deprecated "In use until ReplaceContentOperation is removed." */
    // FIXME(hps,17.12.19) remove this.
    @Deprecated()
    RemotePublicationReceiverService.Configuration getConfiguration();

    /**
     * Traverses through the descendant tree of resource up to the content nodes and writes the versions of the
     * versionables to output
     */
    void traverseTree(Resource resource, Consumer<VersionableInfo> output) throws IOException;

    /** Prepares the temporary directory for an update operation. Take care to remove it later! */
    UpdateInfo startUpdate(String releaseRootPath, String contentPath) throws PersistenceException, LoginException, RemotePublicationReceiverException, RepositoryException;

    /** Uploads one package into the temporary directory, taking note of the root path for later moving to content. */
    void pathUpload(String updateId, String packageRootPath, InputStream inputStream)
            throws LoginException, RemotePublicationReceiverException, RepositoryException, IOException, ConfigurationException;

    /**
     * Moves the content to the content directory and deletes the given paths, thus finalizing the update. The
     * temporary directory is then deleted.
     */
    void commit(String updateId, Set<String> deletedPaths) throws LoginException, RemotePublicationReceiverException, RepositoryException, PersistenceException;

    class RemotePublicationReceiverException extends Exception {

        enum RetryAdvice {
            /** Temporary failure (e.g. because of concurrent modification) - can be retried immediately. */
            RETRY_IMMEDIATELY,
            /** Permanent failure - manual intervention needed. */
            NO_AUTOMATIC_RETRY
        }

        private final RetryAdvice retryadvice;

        public RemotePublicationReceiverException(String message, RetryAdvice advice) {
            super(message);
            this.retryadvice = advice;
        }

        public RetryAdvice getRetryadvice() {
            return retryadvice;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("RemotePublicationFacadeException{");
            sb.append("message='").append(getMessage()).append('\'');
            sb.append(", retryadvice=").append(retryadvice);
            sb.append('}');
            return sb.toString();
        }
    }

}
