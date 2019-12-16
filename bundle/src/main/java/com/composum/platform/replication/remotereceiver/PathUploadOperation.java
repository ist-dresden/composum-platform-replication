package com.composum.platform.replication.remotereceiver;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.SlingResourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.fs.io.Importer;
import org.apache.jackrabbit.vault.fs.io.ZipStreamArchive;
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
import javax.jcr.Session;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_CONTENTPATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_RELEASEROOT_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_UPDATEDPATHS;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_UPDATEID;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PATTERN_UPDATEID;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/** Receives a package and saves it in the temporary folder. */
class PathUploadOperation extends AbstractContentUpdateOperation {

    private static final Logger LOG = LoggerFactory.getLogger(PathUploadOperation.class);

    public PathUploadOperation(Supplier<RemotePublicationReceiverServlet.Configuration> getConfig, ResourceResolverFactory resolverFactory) {
        super(getConfig, resolverFactory);
    }

    @Override
    public void doIt(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response, @Nullable ResourceHandle resource) throws RepositoryException, IOException, ServletException {
        Status status = new Status(request, response);
        try (ResourceResolver resolver = makeResolver()) {
            String packageRootPath = request.getRequestPathInfo().getSuffix();
            String updateId = status.getRequiredParameter(PARAM_UPDATEID, PATTERN_UPDATEID, "PatternId required");
            if (StringUtils.isNotBlank(packageRootPath) && status.isValid()) {
                Resource tmpLocation = getTmpLocation(resolver, updateId, false, status);
                ModifiableValueMap vm = tmpLocation.adaptTo(ModifiableValueMap.class);
                String contentPath = vm.get(ATTR_CONTENTPATH, String.class);
                String releaseRootPath = vm.get(ATTR_RELEASEROOT_PATH, String.class);
                if (SlingResourceUtil.isSameOrDescendant(contentPath, packageRootPath)) {
                    ZipStreamArchive archive = new ZipStreamArchive(request.getInputStream());
                    try {
                        Session session = requireNonNull(resolver.adaptTo(Session.class));

                        Importer importer = new Importer();
                        importer.getOptions().setFilter(new DefaultWorkspaceFilter());
                        archive.open(true);
                        LOG.info("Importing {}", archive.getMetaInf().getProperties());
                        importer.run(archive, session, tmpLocation.getPath());
                        if (importer.hasErrors()) {
                            LOG.error("Aborting: importer has errors. {}", archive.getMetaInf().getProperties());
                            throw new ServletException("Aborting: import on remote system had errors - please consult the " +
                                    "logfile.");
                        }

                        session.save();
                    } finally {
                        archive.close();
                    }

                    List<String> newPaths = new ArrayList<>(asList(vm.get(ATTR_UPDATEDPATHS, new String[0])));
                    newPaths.add(packageRootPath);
                    vm.put(ATTR_UPDATEDPATHS, newPaths);
                    resolver.commit();
                } else {
                    status.withLogging(LOG).validationError("Contraint violated: package root {} not subpath of content root " +
                            "{}", packageRootPath, contentPath);
                }
            } else {
                status.withLogging(LOG).validationError("Broken parameters pkg {}, upd {}", packageRootPath, updateId);
            }
        } catch (LoginException e) { // serious misconfiguration
            LOG.error("Could not get service resolver" + e, e);
            throw new ServletException("Could not get service resolver", e);
        } catch (ConfigurationException e) { // on importer.run
            LOG.error("" + e, e);
            status.error("Import failed.", e);
        }
        status.sendJson();
    }
}
