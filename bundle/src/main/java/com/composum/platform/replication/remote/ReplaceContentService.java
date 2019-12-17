package com.composum.platform.replication.remote;

import com.composum.platform.commons.crypt.CryptoService;
import com.composum.platform.commons.util.LazyInputStream;
import com.composum.platform.replication.remotereceiver.RemotePublicationConfig;
import com.composum.platform.replication.remotereceiver.RemotePublicationReceiverServlet;
import com.composum.platform.replication.remotereceiver.RemoteReceiverConstants;
import com.composum.sling.core.BeanContext;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.nodes.NodesConfiguration;
import com.composum.sling.nodes.servlet.SourceModel;
import com.composum.sling.platform.security.AccessMode;
import com.composum.sling.platform.staging.ReleaseChangeEventListener;
import com.composum.sling.platform.staging.StagingReleaseManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.ZipOutputStream;

/**
 * (Obsolete) Transmits the changes of the JCR content of a release to a remote system.
 * We transmit the subtrees of all resources changed in the event as a zip to the
 * {@link RemotePublicationReceiverServlet}.
 *
 * @deprecated still works, but will be removed after the important parts are extracted.
 */
@Component(
        service = ReleaseChangeEventListener.class,
        name = "Composum Platform Remote Publisher Service (OLD)",
        immediate = true)
@Designate(ocd = ReplaceContentService.Configuration.class)
public class ReplaceContentService implements ReleaseChangeEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(ReplaceContentService.class);
    public static final String PATH_CONFIGROOT = "/conf";
    public static final String DIR_REPLICATION = "/replication";

    protected volatile Configuration config;

    protected volatile CloseableHttpClient httpClient;

    @Reference
    protected NodesConfiguration nodesConfig;

    @Reference
    protected ThreadPoolManager threadPoolManager;

    @Reference
    protected StagingReleaseManager releaseManager;

    @Reference
    protected CryptoService cryptoService;

    protected ThreadPool threadPool;

    @Override
    public void receive(ReleaseChangeEvent event) throws ReplicationFailedException {
        if (!isEnabled()) { return; }
        StagingReleaseManager.Release release = event.release();
        ResourceResolver resolver = releaseManager.getResolverForRelease(release, null, false);
        BeanContext context = new BeanContext.Service(resolver);

        if (release.getMarks().stream().map(AccessMode::accessModeValue).noneMatch((a) -> a.equals(AccessMode.PUBLIC))) {
            return; // TODO possibly other than PUBLIC?
        }

        List<RemotePublicationConfig> replicationConfigs = getReplicationConfigs(release.getReleaseRoot(), context);
        LOG.debug("Replication configurations: {}", replicationConfigs);
        if (replicationConfigs.isEmpty()) { return; }

        Set<String> changedPaths = changedPaths(event);
        LOG.info("Changed paths: {}", changedPaths);

        try {
            List<Exception> exceptionHolder = Collections.synchronizedList(new ArrayList<>());

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            for (String path : changedPaths) {
                Resource resource = resolver.getResource(path);
                if (resource != null) {
                    InputStream pkg = createPkg(context, resource);
                    builder.addTextBody(RemoteReceiverConstants.PARAM_PATH, path);
                    builder.addBinaryBody("file", pkg);
                } else {
                    builder.addTextBody(RemoteReceiverConstants.PARAM_DELETED_PATH, path);
                }
            }
            // deliberately as last parameter, mandatory and is also used to ensure request was transmitted completely:
            builder.addTextBody(RemoteReceiverConstants.PARAM_RELEASEROOT, release.getReleaseRoot().getPath());

            for (RemotePublicationConfig replicationConfig : replicationConfigs) {
                HttpClientContext httpClientContext = replicationConfig.initHttpContext(HttpClientContext.create(),
                        passwordDecryptor());
                HttpPost post = new HttpPost(replicationConfig.getReceiverUri() + ".replaceContent.zip");
                post.setEntity(builder.build());
                try (CloseableHttpResponse response = httpClient.execute(post, httpClientContext)) {
                    StatusLine statusLine = response.getStatusLine();
                    if (statusLine.getStatusCode() < 200 || statusLine.getStatusCode() >= 300) {
                        LOG.error("Failure response {}, {}", statusLine.getStatusCode(), statusLine.getReasonPhrase());
                        throw new ReplicationFailedException("Could not publish to remote service because of "
                                + statusLine.getStatusCode() + " : " + statusLine.getReasonPhrase(),
                                null, event);
                    }
                    LOG.info("Remote replication successful with {}, {}", statusLine.getStatusCode(), statusLine.getReasonPhrase());
                    if (!exceptionHolder.isEmpty()) {
                        // FIXME(hps,21.11.19) Ouch! That would be really troublesome! How to prevent such a situation?
                        ReplicationFailedException replicationFailedException =
                                new ReplicationFailedException("Remote publishing succeed but with broken transmitted " +
                                        "content! Please repeat - there might be broken content at the other side.",
                                        exceptionHolder.get(0), event);
                        for (Exception exception : exceptionHolder) {
                            LOG.error("Generating the written zip yielded an exception", exception);
                            //noinspection ObjectEquality
                            if (exception != exceptionHolder.get(0)) {
                                replicationFailedException.addSuppressed(exception);
                            }
                        }
                        throw replicationFailedException;
                    }
                }
            }
        } catch (IOException | RuntimeException | RepositoryException e) {
            LOG.error("Remote publishing failed", e);
            throw new ReplicationFailedException("Remote publishing failed", e, event);
        }
    }

    public Function<String, String> passwordDecryptor() {
        Function<String, String> decryptor = null;
        String key = config.configurationPassword();
        if (StringUtils.isNotBlank(key)) {
            decryptor = (password) -> cryptoService.decrypt(password, key);
        }
        return decryptor;
    }

    /**
     * Try to avoid creating the whole zip in memory. Not quite right yet, since that might fill up the pool
     * if there are many things to write.
     */
    @Deprecated
    protected InputStream startZipWriting(BeanContext context, Set<String> changedPaths,
                                          Exception[] exceptionHolder) {
        PipedInputStream zipContent = new PipedInputStream();
        threadPool.execute(() -> {
            try (OutputStream writeZipStream = new PipedOutputStream(zipContent);
                 ZipOutputStream zipStream = new ZipOutputStream(writeZipStream)) {
                writePathsToZip(context, changedPaths, zipStream);
                zipStream.flush();
            } catch (RepositoryException | IOException | RuntimeException e) {
                exceptionHolder[0] = e;
            }
        });
        return zipContent;
    }

    @Nonnull
    protected InputStream createPkg(@Nonnull BeanContext context, @Nonnull Resource resource) throws RepositoryException, IOException {
        SourceModel model = new SourceModel(nodesConfig, context, resource);
        return new LazyInputStream(() -> {
            // TODO avoid creating the whole page in memory - something like startZipWriting?
            ByteArrayOutputStream writeZipStream = new ByteArrayOutputStream();
            model.writePackage(writeZipStream, "remotepublisher", resource.getPath(), "1");
            return new ByteArrayInputStream(writeZipStream.toByteArray());
        });
    }

    protected void writePathsToZip(BeanContext context, Set<String> changedPaths, ZipOutputStream zipStream) throws IOException, RepositoryException {
        for (String path : changedPaths) {
            Resource resource = context.getResolver().getResource(path);
            if (resource != null) {
                SourceModel model = new SourceModel(nodesConfig, context, resource);
                model.writeZip(zipStream, resource.getPath(), true);
            }
        }
    }

    protected Set<String> changedPaths(ReleaseChangeEvent event) {
        Set<String> changedPaths = new LinkedHashSet<>();
        changedPaths.addAll(event.newOrMovedResources());
        changedPaths.addAll(event.removedOrMovedResources());
        changedPaths.addAll(event.updatedResources());
        changedPaths = cleanupPaths(changedPaths);
        return changedPaths;
    }

    /** Removes paths that are contained in other paths. */
    protected Set<String> cleanupPaths(Set<String> changedPaths) {
        Set<String> cleanedPaths = new LinkedHashSet<>();
        for (String path : changedPaths) {
            cleanedPaths.removeIf((p) -> SlingResourceUtil.isSameOrDescendant(path, p));
            if (cleanedPaths.stream().noneMatch((p) -> SlingResourceUtil.isSameOrDescendant(p, path))) {
                cleanedPaths.add(path);
            }
        }
        return cleanedPaths;
    }

    protected List<RemotePublicationConfig> getReplicationConfigs(@Nonnull Resource releaseRoot,
                                                                  @Nonnull BeanContext context) {
        String releasePath = releaseRoot.getPath();
        if (releasePath.startsWith("/content/")) { releasePath = StringUtils.removeStart(releasePath, "/content"); }
        String configparent = PATH_CONFIGROOT + releasePath + DIR_REPLICATION;

        List<RemotePublicationConfig> configs = new ArrayList<>();
        Resource configroot = releaseRoot.getResourceResolver().getResource(configparent);
        if (configroot != null) {
            for (Resource child : configroot.getChildren()) {
                RemotePublicationConfig replicationConfig =
                        context.withResource(child).adaptTo(RemotePublicationConfig.class);
                if (replicationConfig != null) { configs.add(replicationConfig); }
            }
        }
        return configs;
    }

    @Activate
    @Modified
    protected void activate(final Configuration theConfig) {
        LOG.info("activated");
        this.config = theConfig;
        this.httpClient = HttpClients.createDefault();
        this.threadPool = threadPoolManager.get(ReplaceContentService.class.getName());
    }

    @Deactivate
    protected void deactivate() throws IOException {
        LOG.info("deactivated");
        try (CloseableHttpClient ignored = httpClient) { // just make sure it's closed.
            ThreadPool oldThreadPool = this.threadPool;
            this.config = null;
            this.httpClient = null;
            this.threadPool = null;
            if (oldThreadPool != null) { threadPoolManager.release(threadPool); }
        }
    }

    protected boolean isEnabled() {
        ReplaceContentService.Configuration theconfig = this.config;
        return theconfig != null && theconfig.enabled();
    }

    @ObjectClassDefinition(
            name = "Composum Platform Remote Publisher Service Configuration (OLD)",
            description = "Configures a service that publishes release changes to remote systems"
    )
    public @interface Configuration {

        @AttributeDefinition(
                description = "the general on/off switch for this service"
        )
        boolean enabled() default false;

        @AttributeDefinition(
                description = "Password to encrypt the passwords of remote receivers."
        )
        String configurationPassword() default "";

    }

}
