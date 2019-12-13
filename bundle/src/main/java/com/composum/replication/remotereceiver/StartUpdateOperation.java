package com.composum.replication.remotereceiver;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.servlet.ServletException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.function.Supplier;

/** Creates a temporary directory to unpack stuff to replace our content. */
public class StartUpdateOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(StartUpdateOperation.class);

    /** Random number generator for creating unique ids etc. */
    protected final Random random;

    StartUpdateOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                         @Nonnull ResourceResolverFactory resolverFactory) {
        super(getConfig, resolverFactory);

        Random therandom;
        try {
            therandom = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) { // should be pretty much impossible
            LOG.error("" + e, e);
            therandom = new Random();
        }
        random = therandom;
    }

    @Override
    public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle ignored)
            throws RepositoryException, IOException, ServletException {
        try (ResourceResolver resolver = makeResolver()) {
            StatusWithReleaseData status = new StatusWithReleaseData(request, response);
            String contentPath = request.getRequestPathInfo().getSuffix();
            String releaseRootPath = request.getParameter(RemoteReceiverConstants.PARAM_RELEASEROOT);
            if (StringUtils.isNotBlank(releaseRootPath) && StringUtils.isNotBlank(contentPath) &&
                    SlingResourceUtil.isSameOrDescendant(releaseRootPath, contentPath)) {

                UpdateInfo updateInfo = new UpdateInfo();
                updateInfo.updateId = "upd-" + RandomStringUtils.random(12, 0, 0, true, true, null, random);
                Resource tmpLocation = getTmpLocation(resolver, updateInfo.updateId, true);
                ModifiableValueMap vm = tmpLocation.adaptTo(ModifiableValueMap.class);
                vm.put(RemoteReceiverConstants.ATTR_CONTENTPATH, contentPath);
                vm.put(RemoteReceiverConstants.ATTR_RELEASEROOT_PATH, releaseRootPath);

                String releaseChangeId = getReleaseChangeId(resolver, contentPath);
                if (releaseChangeId != null) {
                    vm.put(RemoteReceiverConstants.ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, releaseChangeId);
                }
                updateInfo.originalPublisherReleaseChangeId = releaseChangeId;
                status.updateInfo = updateInfo;
                resolver.commit();
            } else {
                status.withLogging(LOG).error("Broken parameters {} : {}", releaseRootPath, contentPath);
            }
            status.sendJson();
        } catch (LoginException e) { // serious misconfiguration
            LOG.error("Could not get service resolver" + e, e);
            throw new ServletException("Could not get service resolver", e);
        }
    }

    protected String getReleaseChangeId(ResourceResolver resolver, String contentPath) {
        Resource resource = resolver.getResource(contentPath);
        return resource != null ? resource.getValueMap().get(StagingConstants.PROP_REPLICATED_VERSION, String.class) : null;
    }

    public static class UpdateInfo {

        /**
         * The update id for the pending operation - has to be named like
         * {@link AbstractContentUpdateOperation#PARAM_UPDATEID}.
         */
        String updateId;

        /** The original status of the publishers release. */
        String originalPublisherReleaseChangeId;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("UpdateInfo{");
            sb.append("updateId='").append(updateId).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    /** Reads the result of {@link StartUpdateOperation} into memory. */
    public static class StatusWithReleaseData extends Status {

        /** The created update data - has to be named like {@link AbstractContentUpdateOperation#DATAFIELD_NAME}. */
        UpdateInfo updateInfo;

        public StatusWithReleaseData() {
            super(null, null);
        }

        public StatusWithReleaseData(SlingHttpServletRequest request, SlingHttpServletResponse response) {
            super(request, response);
        }

        @Override
        public boolean isValid() {
            return super.isValid() && updateInfo != null && updateInfo.updateId != null;
        }
    }
}
