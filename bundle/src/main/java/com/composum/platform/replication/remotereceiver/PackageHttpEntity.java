package com.composum.platform.replication.remotereceiver;

import com.composum.sling.core.BeanContext;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.nodes.servlet.SourceModel;
import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** An {@link HttpEntity} that generates on the fly a package and writes it into the request. */
public class PackageHttpEntity extends AbstractHttpEntity implements HttpEntity {

    private static final Logger LOG = LoggerFactory.getLogger(PackageHttpEntity.class);

    private final NodesConfiguration nodesConfig;
    private final BeanContext context;
    private final Resource resource;

    /** @param resource that is the top-level of the package. */
    public PackageHttpEntity(@Nonnull NodesConfiguration nodesConfig,
                             @Nonnull BeanContext context, @Nonnull Resource resource) {
        setContentEncoding(StandardCharsets.UTF_8.name());
        setContentType("application/zip");
        this.resource = resource;
        this.nodesConfig = nodesConfig;
        this.context = context;
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {
        try {
            SourceModel model = new SourceModel(nodesConfig, context, resource);
            model.writePackage(outstream, "remotepublisher", resource.getPath(), "1");
        } catch (RepositoryException e) {
            LOG.error("Trouble creating package for {}", SlingResourceUtil.getPath(resource));
            throw new IOException("Could not create package for " + SlingResourceUtil.getPath(resource), e);
        }
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    /** Not implemented. */
    @Override
    public InputStream getContent() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("PackageHttpEntity only supports writeTo(OutputStream).");
    }

}
