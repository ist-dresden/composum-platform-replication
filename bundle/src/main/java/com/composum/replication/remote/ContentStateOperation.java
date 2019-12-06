package com.composum.replication.remote;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.google.common.collect.ImmutableMap;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Returns the state of the content of a subtree of a site or the whole site as JSON, including parent node
 * orderings and child node version uuids.
 */
public class ContentStateOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ContentStateOperation.class);

    public ContentStateOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                                 @Nonnull ResourceResolverFactory resolverFactory) {
        super(getConfig, resolverFactory);
    }

    @Override
    public void doIt(SlingHttpServletRequest request, SlingHttpServletResponse response, ResourceHandle resource) throws RepositoryException, IOException, ServletException {
        Status status = new Status(request, response);
        status.withLogging(LOG).info("Test.");
        status.setTitle("title");
        status.list("alist").add(
                ImmutableMap.of("hallo", Arrays.asList("bla", "blu"), "themap", ImmutableMap.of("k1", "v1", "k2", "v2")));
        status.data("thedata").put("bla", "blu");
        status.sendJson(HttpServletResponse.SC_OK);
    }
}
