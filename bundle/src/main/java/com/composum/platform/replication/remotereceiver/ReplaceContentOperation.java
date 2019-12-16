package com.composum.platform.replication.remotereceiver;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.ServletOperation;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.platform.staging.impl.NodeTreeSynchronizer;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.fs.io.Importer;
import org.apache.jackrabbit.vault.fs.io.MemoryArchive;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.request.RequestParameter;
import org.apache.sling.api.request.RequestParameterMap;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
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
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.composum.sling.core.servlet.AbstractServiceServlet.PARAM_FILE;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_DELETED_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PARAM_RELEASEROOT;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.removeStart;

/**
 * Receives a zip that transactionally replaces a part of the content - for use within the
 * {@link RemotePublicationReceiverServlet}.
 * @deprecated to be replaced
 */
public class ReplaceContentOperation implements ServletOperation {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceContentOperation.class);

    /** Attribute on the temporary directory that has a list of paths which should be deleted from the content. */
    protected static final String ATTR_PATHSTODELETE = "todelete";

    /**
     * Attribute on the temporary directory that has a list of paths which should be updated in the content.
     * Each of these contains a whole subtree that should replace the corresponding path in the current replicated
     * content.
     */
    protected static final String ATTR_PATHSTOUPDATE = "toupdate";

    @Nonnull
    protected final Supplier<RemotePublicationReceiverServlet.Configuration> configSupplier;

    @Nonnull
    protected ResourceResolverFactory resolverFactory;

    protected final NodeTreeSynchronizer nodeTreeSynchronizer = new NodeTreeSynchronizer();

    public ReplaceContentOperation(@Nonnull Supplier<RemotePublicationReceiverServlet.Configuration> getConfig,
                                   @Nonnull ResourceResolverFactory resolverFactory) {
        this.configSupplier = getConfig;
        this.resolverFactory = resolverFactory;
    }

    @Override
    public void doIt(SlingHttpServletRequest request, SlingHttpServletResponse response, ResourceHandle resource)
            throws RepositoryException, IOException, ServletException {
        final String tmpLocation =
                configSupplier.get().tmpDir() + "/imp-" + RandomStringUtils.randomAlphanumeric(12);
        RequestParameterMap parameters = request.getRequestParameterMap();
        List<RequestParameter> fileParams = toList(parameters.getValues(PARAM_FILE));
        List<RequestParameter> pathParams = toList(parameters.getValues(PARAM_PATH));
        List<RequestParameter> deletedPathParams = toList(parameters.getValues(PARAM_DELETED_PATH));
        RequestParameter releaseRootParameter = parameters.getValue(PARAM_RELEASEROOT);
        String releaseRootPath = releaseRootParameter != null ?
                releaseRootParameter.getString(StandardCharsets.UTF_8.name()) : null;
        if (releaseRootParameter == null) {
            LOG.error("Final parameter {} missing - broken request received, aborted.", PARAM_RELEASEROOT);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Final parameter " + releaseRootParameter + " missing - broken request received, aborted.");
            return;
        }

        LOG.info("Root {}, Paths: {} Deleted Paths: {}", releaseRootPath, pathParams, deletedPathParams);

        ResourceHandle tmpdir = null;
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(null)) {
            try {
                tmpdir = ResourceHandle.use(ResourceUtil.getOrCreateResource(resolver, tmpLocation));
                tmpdir.setProperty(ResourceUtil.PROP_MIXINTYPES, Collections.singletonList(ResourceUtil.TYPE_CREATED));
                Session session = requireNonNull(resolver.adaptTo(Session.class));

                if (!deletedPathParams.isEmpty()) {
                    List<String> deletedPaths =
                            deletedPathParams.stream().map(RequestParameter::getString).collect(Collectors.toList());
                    tmpdir.setProperty(ATTR_PATHSTODELETE, deletedPaths);
                    resolver.commit();
                }

                if (!pathParams.isEmpty()) {
                    List<String> updatedPaths =
                            pathParams.stream().map(RequestParameter::getString).collect(Collectors.toList());
                    tmpdir.setProperty(ATTR_PATHSTOUPDATE, updatedPaths);
                    resolver.commit();
                }

                for (RequestParameter file : fileParams) {
                    try (InputStream inputStream = file.getInputStream()) {
                        MemoryArchive archive = new MemoryArchive(false);
                        try {
                            archive.run(requireNonNull(inputStream));
                            Importer importer = new Importer();
                            importer.getOptions().setFilter(new DefaultWorkspaceFilter());
                            archive.open(true);
                            LOG.info("Importing {}", archive.getMetaInf().getProperties());
                            importer.run(archive, session, tmpLocation);
                            if (importer.hasErrors()) {
                                LOG.error("Aborting: importer has errors. {}", archive.getMetaInf().getProperties());
                                throw new ServletException("Aborting: import on remote system had errors - please consult the " +
                                        "logfile.");
                            }
                            session.save();
                        } finally {
                            archive.close();
                        }
                    }
                }

                resolver.commit(); // make sure everything is writeable.

                updateContent(tmpdir, resolver, releaseRootPath);
                resolver.delete(tmpdir);
                workaroundForCommit(session, releaseRootPath);
                resolver.commit();
                tmpdir = null;
            } finally {
                resolver.revert();
                tmpdir = ResourceHandle.use(resolver.getResource(tmpLocation));
                if (tmpdir.isValid()) {
                    resolver.delete(tmpdir);
                    resolver.commit();
                }
            }
        } catch (ServletException e) {
            throw e;
        } catch (LoginException e) {
            LOG.error("Cannot get service resolver", e);
            throw new ServletException("Cannot get service resolver", e);
        } catch (Exception e) {
            LOG.error(e.toString(), e);
            throw new ServletException(e);
        }
    }

    /** Writes the imported content parts to the replicated content. */
    protected void updateContent(@Nonnull ResourceHandle importDir, ResourceResolver resolver, String releaseRootPath) throws PersistenceException, RepositoryException {
        Session session = requireNonNull(resolver.adaptTo(Session.class));
        String[] deletedPaths = importDir.getProperty(ATTR_PATHSTODELETE, new String[0]);
        String[] updatedPaths = importDir.getProperty(ATTR_PATHSTOUPDATE, new String[0]);
        String targetDir = configSupplier.get().targetDir();

        LOG.info("Deleting {}", Arrays.asList(deletedPaths));
        for (String absolutePath : deletedPaths) {
            String path = removeStart(absolutePath, "/");
            Resource resource = resolver.getResource(SlingResourceUtil.appendPaths(targetDir, path));
            if (resource != null) {
                resolver.delete(resource);
            } else { // Strange. Shouldn't normally happen.
                LOG.info("Path was not present: {}", path);
            }
        }

        LOG.info("Updating {}", Arrays.asList(updatedPaths));
        for (String absolutePath : updatedPaths) {
            String path = removeStart(absolutePath, "/");
            Resource src = requireNonNull(importDir.getChild(path), path);
            String destPath = SlingResourceUtil.appendPaths(targetDir, path);
            ensureNodes(ResourceUtil.getParent(path), importDir, resolver.getResource(targetDir), resolver, releaseRootPath);
            Resource dst = resolver.getResource(destPath);
            if (dst != null) { session.removeItem(destPath); }
            session.move(src.getPath(), destPath);
        }

        removeOrphans(resolver, releaseRootPath);
    }


    /** Creates nodes until contentRoot.getChild(path) from the template importDir.getChild(path). */
    @Nonnull
    protected Resource ensureNodes(@Nullable String path, @Nonnull ResourceHandle importDir, @Nonnull Resource contentRoot,
                                   @Nonnull ResourceResolver resolver, @Nonnull String releaseRootPath) throws RepositoryException, PersistenceException {
        if (path == null || path.isEmpty() || path.equals("/")) { return contentRoot; }
        String parent = ResourceUtil.getParent(path);
        Resource contentParent = ensureNodes(parent, importDir, contentRoot, resolver, releaseRootPath);
        Resource contentNode = contentParent.getChild(ResourceUtil.getName(path));
        boolean attributeUpdateNeeded = StringUtils.startsWith(path, releaseRootPath);
        if (contentNode == null) { // create it and fixup attributes in the next step.
            contentNode = resolver.create(contentParent, ResourceUtil.getName(path),
                    ImmutableMap.of(ResourceUtil.PROP_PRIMARY_TYPE, ResourceUtil.TYPE_SLING_FOLDER));
            attributeUpdateNeeded = true;
        }
        if (attributeUpdateNeeded) {
            nodeTreeSynchronizer.updateAttributes(ResourceHandle.use(importDir.getChild(path)),
                    ResourceHandle.use(contentNode), ImmutableBiMap.of());
        }
        return contentNode;
    }

    /** Removes all descendants of releaseRoot that do not have a versionable as descendant. */
    protected void removeOrphans(ResourceResolver resolver, String releaseRootPath) throws PersistenceException {
        if (!"/".equals(configSupplier.get().targetDir().trim())) {
            releaseRootPath = configSupplier.get().targetDir() + releaseRootPath;
        }
        Resource releaseRoot = resolver.getResource(releaseRootPath);
        if (releaseRoot != null) { removeOrphans(releaseRoot); }
    }

    /**
     * Recursiely removes descendants of root that do not have versionables as descendants. Returns true if root is a versionable
     * or has versionable descendants. (Of course, this stops at versionables.)
     */
    protected boolean removeOrphans(Resource root) throws PersistenceException {
        if (ResourceUtil.isNodeType(root, ResourceUtil.MIX_VERSIONABLE)) { return true; }
        boolean hasVersionableDescendants = false;
        for (Resource child : IterableUtils.toList(root.getChildren())) {
            if (removeOrphans(child)) { hasVersionableDescendants = true; }
        }
        if (!hasVersionableDescendants) {
            root.getResourceResolver().delete(root);
        }
        return hasVersionableDescendants;
    }

    /**
     * Workaround for nasty OAK bug that throws an "CommitFailedException: OakConstraint0100: Property is protected:
     * jcr:versionHistory" if we exchange a versionable node with a copy of it. It thinks we modified protected
     * properties of that node, while not recognizing we have a new node now. See
     * {@link com.composum.platform.replication.remote.ReplacementStrategyExploration#bugWithReplace()} .
     * Workaround here is to rename it for a short while, tricking it to see it as a new node, and rename it
     * immediately back. This is a bad workaround, because there is a small time during which requests will fail.
     */
    // FIXME(hps,03.12.19) use a different way
    protected void workaroundForCommit(Session session, String releaseRootPath) throws RepositoryException {
        if (!"/".equals(configSupplier.get().targetDir().trim())) {
            releaseRootPath = configSupplier.get().targetDir() + releaseRootPath;
        }
        String tmpPath = releaseRootPath + "-" + RandomStringUtils.randomAlphanumeric(8);
        if (!session.itemExists(releaseRootPath)) { return; }
        session.move(releaseRootPath, tmpPath);
        session.save();
        try {
            session.move(tmpPath, releaseRootPath);
            session.save();
        } catch (RepositoryException | RuntimeException e) {
            LOG.error("CAUTION: human intervention neccesary. We renamed {} to {} but could not rename it back.",
                    releaseRootPath, tmpPath);
            throw e;
        }
    }

    @Nonnull
    protected List<RequestParameter> toList(@Nullable RequestParameter[] values) {
        return Arrays.asList(values != null ? values : new RequestParameter[0]);
    }
}
