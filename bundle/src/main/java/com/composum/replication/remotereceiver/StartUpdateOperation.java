package com.composum.replication.remotereceiver;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
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
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

/** Creates a temporary directory to unpack stuff to replace our content. */
public class StartUpdateOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(StartUpdateOperation.class);

    /** Random number generator for creating unique ids etc. */
    protected final Random random = SecureRandom.getInstanceStrong();

    StartUpdateOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                         @Nonnull ResourceResolverFactory resolverFactory) throws NoSuchAlgorithmException {
        super(getConfig, resolverFactory);
    }

    @Override
    public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource) throws RepositoryException, IOException, ServletException {
        Status status = new Status(request, response);
        if (resource != null && resource.isValid()) {
            ResourceResolver resolver = request.getResourceResolver(); // FIXME(hps,11.12.19) service resolver
            String updateid = "upd-" + RandomStringUtils.random(12, 0, 0, true, true, null, random);
            Resource tmpLocation = getTmpLocation(resolver, updateid, true);
            Map<String, Object> updateData = status.data(DATAFIELD_NAME);
            updateData.put(PARAM_UPDATEID, updateid);
        } else {
            status.withLogging(LOG).error("No readable path given as suffix: {}", request.getRequestPathInfo().getSuffix());
        }
        status.sendJson();
    }

}
