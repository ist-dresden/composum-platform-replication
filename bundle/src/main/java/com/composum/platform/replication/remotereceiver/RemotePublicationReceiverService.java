package com.composum.platform.replication.remotereceiver;

import com.composum.platform.replication.json.VersionableInfo;
import com.composum.platform.replication.json.VersionableTree;
import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.core.util.SlingResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import com.composum.sling.platform.staging.impl.NodeTreeSynchronizer;
import com.google.common.collect.ImmutableBiMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.vault.fs.config.ConfigurationException;
import org.apache.jackrabbit.vault.fs.config.DefaultWorkspaceFilter;
import org.apache.jackrabbit.vault.fs.io.Importer;
import org.apache.jackrabbit.vault.fs.io.ZipStreamArchive;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.Constants;
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
import javax.jcr.Session;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_RELEASEROOT_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_TOP_CONTENTPATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_UPDATEDPATHS;
import static com.composum.sling.core.util.SlingResourceUtil.appendPaths;
import static com.composum.sling.platform.staging.StagingConstants.PROP_REPLICATED_VERSION;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/** Interface for service that implements the functions behind the {@link RemotePublicationReceiverServlet}. */
@Component(
        service = RemotePublicationReceiver.class,
        property = {Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Receiver Service"})
@Designate(ocd = RemotePublicationReceiverService.Configuration.class)
public class RemotePublicationReceiverService implements RemotePublicationReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverService.class);

    protected volatile Configuration config;

    @Reference
    protected ResourceResolverFactory resolverFactory;

    /** Random number generator for creating unique ids etc. */
    protected final Random random;

    public RemotePublicationReceiverService() {
        Random therandom;
        try {
            therandom = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) { // should be pretty much impossible
            LOG.error("" + e, e);
            therandom = new Random();
        }
        random = therandom;
    }

    @Activate
    @Modified
    protected void activate(Configuration configuration) {
        this.config = configuration;
    }

    @Deactivate
    protected void deactivate() {
        this.config = null;
    }

    @Override
    public boolean isEnabled() {
        Configuration theconfig = this.config;
        return theconfig != null && theconfig.enabled();
    }

    @Override
    public String getTargetDir() {
        return config.targetDir();
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public void traverseTree(Resource resource, Consumer<VersionableInfo> output) throws IOException {
        if (resource == null) { return; }
        if (ResourceUtil.isNodeType(resource, ResourceUtil.TYPE_VERSIONABLE)) {
            VersionableInfo info = VersionableInfo.of(resource, null);
            if (info != null) { output.accept(info); }
        } else if (ResourceUtil.CONTENT_NODE.equals(resource.getName())) {
            // that shouldn't happen in the intended usecase: non-versionable jcr:content
            LOG.warn("Something's wrong here: {} has no {}", resource.getPath(), PROP_REPLICATED_VERSION);
        } else { // traverse tree
            for (Resource child : resource.getChildren()) {
                traverseTree(child, output);
            }
        }
    }

    @Override
    public UpdateInfo startUpdate(String releaseRootPath, String contentPath)
            throws PersistenceException, LoginException, RemotePublicationReceiverException, RepositoryException {
        LOG.info("Commit called for {} : {}", releaseRootPath, contentPath);
        try (ResourceResolver resolver = makeResolver()) {

            UpdateInfo updateInfo = new UpdateInfo();
            updateInfo.updateId = "upd-" + RandomStringUtils.random(12, 0, 0, true, true, null, random);
            assert RemoteReceiverConstants.PATTERN_UPDATEID.matcher(updateInfo.updateId).matches();
            Resource tmpLocation = getTmpLocation(resolver, updateInfo.updateId, true);

            ModifiableValueMap vm = requireNonNull(tmpLocation.adaptTo(ModifiableValueMap.class));
            if (SlingResourceUtil.isSameOrDescendant(releaseRootPath, contentPath)) {
                vm.put(RemoteReceiverConstants.ATTR_TOP_CONTENTPATH, contentPath);
                vm.put(RemoteReceiverConstants.ATTR_RELEASEROOT_PATH, releaseRootPath);
            } else { // weird internal error - doesn't make sense to put that to the user.
                throw new IllegalArgumentException("Contraint violated: content path " + contentPath + " not " +
                        "subpath of release root " + releaseRootPath);
            }

            String releaseChangeId = getReleaseChangeId(resolver, contentPath);
            if (releaseChangeId != null) {
                vm.put(RemoteReceiverConstants.ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, releaseChangeId);
            }
            updateInfo.originalPublisherReleaseChangeId = releaseChangeId;
            resolver.commit();
            return updateInfo;
        }
    }

    @Nonnull
    @Override
    public List<String> compareContent(String updateId, @Nonnull InputStream jsonInputStream)
            throws LoginException, RemotePublicationReceiverException, RepositoryException, IOException {
        LOG.info("Compare content {}", updateId);
        try (ResourceResolver resolver = makeResolver();
             Reader json = new InputStreamReader(jsonInputStream, StandardCharsets.UTF_8)) {
            Resource tmpLocation = getTmpLocation(resolver, updateId, false);
            String contentPath = tmpLocation.getValueMap().get(ATTR_TOP_CONTENTPATH, String.class);
            VersionableTree.VersionableTreeDeserializer factory =
                    new VersionableTree.VersionableTreeDeserializer(config.targetDir(), resolver, contentPath);
            Gson gson = new GsonBuilder().registerTypeAdapterFactory(factory).create();
            VersionableTree versionableTree = gson.fromJson(json, VersionableTree.class);
            List<String> result = new ArrayList<>();
            result.addAll(versionableTree.getChangedPaths());
            result.addAll(versionableTree.getDeletedPaths());
            LOG.info("Different versionables: {}", result);
            return result;
        }
    }

    @Override
    public void pathUpload(String updateId, String packageRootPath, InputStream inputStream)
            throws LoginException, RemotePublicationReceiverException, RepositoryException, IOException, ConfigurationException {
        LOG.info("Pathupload called for {} : {}", updateId, packageRootPath);
        try (ResourceResolver resolver = makeResolver()) {
            Resource tmpLocation = getTmpLocation(resolver, updateId, false);
            ModifiableValueMap vm = requireNonNull(tmpLocation.adaptTo(ModifiableValueMap.class));
            String contentPath = vm.get(ATTR_TOP_CONTENTPATH, String.class);
            if (SlingResourceUtil.isSameOrDescendant(contentPath, packageRootPath)) {
                ZipStreamArchive archive = new ZipStreamArchive(inputStream);
                try {
                    Session session = requireNonNull(resolver.adaptTo(Session.class));

                    Importer importer = new Importer();
                    importer.getOptions().setFilter(new DefaultWorkspaceFilter());
                    archive.open(true);
                    LOG.info("Importing {}", archive.getMetaInf().getProperties());
                    importer.run(archive, session, tmpLocation.getPath());
                    if (importer.hasErrors()) {
                        LOG.error("Aborting import on {} to {}: importer has errors. {}",
                                updateId, packageRootPath, archive.getMetaInf().getProperties());
                        throw new RemotePublicationReceiverException("Aborting: internal error importing on remote " +
                                "system - please consult the logfile.", RemotePublicationReceiverException.RetryAdvice.NO_AUTOMATIC_RETRY);
                    }

                    session.save();
                } finally {
                    archive.close();
                }

                List<String> newPaths = new ArrayList<>(asList(vm.get(ATTR_UPDATEDPATHS, new String[0])));
                newPaths.add(packageRootPath);
                vm.put(ATTR_UPDATEDPATHS, newPaths.toArray(new String[0]));
                resolver.commit();
            } else { // weird internal error - doesn't make sense to put that to the user.
                LOG.error("Contraint violated: package root {} not subpath of content root {}", packageRootPath, contentPath);
                throw new IllegalArgumentException("Contraint violated: package root " + packageRootPath + " not " +
                        "subpath of content root " + contentPath);
            }
        }
    }

    @Override
    public void commit(String updateId, Set<String> deletedPaths)
            throws LoginException, RemotePublicationReceiverException, RepositoryException, PersistenceException {
        LOG.info("Commit called for {} : {}", updateId, deletedPaths);
        try (ResourceResolver resolver = makeResolver()) {
            Resource tmpLocation = getTmpLocation(resolver, updateId, false);
            ValueMap vm = tmpLocation.getValueMap();
            String topContentPath = vm.get(ATTR_TOP_CONTENTPATH, String.class);
            String releaseRootPath = requireNonNull(vm.get(ATTR_RELEASEROOT_PATH, String.class));
            String targetRoot = requireNonNull(config.targetDir());
            String @NotNull [] updatedPaths = vm.get(ATTR_UPDATEDPATHS, new String[0]);

            for (String deletedPath : deletedPaths) {
                if (!SlingResourceUtil.isSameOrDescendant(topContentPath, deletedPath)) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + topContentPath + " : " + deletedPath);
                }
                deletePath(resolver, targetRoot, deletedPath);
            }

            String targetReleaseRootPath = appendPaths(targetRoot, releaseRootPath);
            ResourceUtil.getOrCreateResource(resolver, targetReleaseRootPath);

            for (String updatedPath : updatedPaths) {
                if (!SlingResourceUtil.isSameOrDescendant(topContentPath, updatedPath)) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + topContentPath + " : " + updatedPath);
                }
                moveVersionable(resolver, tmpLocation, updatedPath, targetRoot);
            }

            for (String deletedPath : deletedPaths) {
                removeOrphans(resolver, targetRoot, deletedPath, targetReleaseRootPath);
            }

            resolver.commit();
        }
    }

    /**
     * Move tmpLocation/updatedPath to targetRoot/updatedPath possibly copying parents if they don't exist.
     * We rely on that the paths have been checked by the caller to not go outside of the release, and that
     * the release in the target has been created.
     */
    protected void moveVersionable(@Nonnull ResourceResolver resolver, @Nonnull Resource tmpLocation,
                                   @Nonnull String updatedPath, @Nonnull String targetRoot)
            throws RepositoryException, PersistenceException {
        NodeTreeSynchronizer synchronizer = new NodeTreeSynchronizer();
        Resource source = tmpLocation;
        Resource destination = requireNonNull(resolver.getResource(targetRoot), targetRoot);
        for (String pathsegment : StringUtils.removeStart(ResourceUtil.getParent(updatedPath), "/").split("/")) {
            source = requireNonNull(source.getChild(pathsegment), updatedPath);
            destination = ResourceUtil.getOrCreateChild(destination, pathsegment, ResourceUtil.TYPE_UNSTRUCTURED);
            synchronizer.updateAttributes(ResourceHandle.use(source), ResourceHandle.use(destination), ImmutableBiMap.of());
        }
        String nodename = ResourceUtil.getName(updatedPath);
        source = requireNonNull(source.getChild(nodename), updatedPath);
        Resource destinationParent = destination;
        destination = destination.getChild(nodename);

        if (destination != null) {
            // can't replace the node since OAK wrongly thinks we changed protected attributes
            // see com.composum.platform.replication.remote.ReplacementStrategyExplorationTest.bugWithReplace
            // we copy the attributes and move the children, instead, so protected attributes stay the same.
            synchronizer.updateAttributes(ResourceHandle.use(source), ResourceHandle.use(destination), ImmutableBiMap.of());
            for (Resource previousChild : destination.getChildren()) {
                resolver.delete(previousChild);
            }
            for (Resource child : source.getChildren()) {
                resolver.move(child.getPath(), destination.getPath());
            }
        } else {
            Resource sourceParent = source.getParent();
            resolver.move(source.getPath(), destinationParent.getPath());
            if (ResourceUtil.isFile(sourceParent) && !sourceParent.hasChildren()) {
                resolver.delete(sourceParent); // otherwise it would be inconsistent.
                // TODO we can remove this when the tmpdir is deleted afterwards.
            }
        }

        LOG.info("Moved {} to {}", SlingResourceUtil.getPath(source),
                SlingResourceUtil.getPath(destinationParent) + "/" + nodename);
    }

    protected void deletePath(@Nonnull ResourceResolver resolver, @Nonnull String targetRoot,
                              @Nonnull String deletedPath) throws PersistenceException {
        Resource deletedResource = resolver.getResource(appendPaths(targetRoot, deletedPath));
        if (deletedResource != null) {
            LOG.info("Deleting {}", deletedPath);
            resolver.delete(deletedResource);
        } else { // some problem with the algorithm!
            LOG.warn("Path to delete unexpectedly not present in content: {}", deletedPath);
        }
    }

    /** Removes parent nodes of the deleted nodes that do not have any (versionable) children now. */
    protected void removeOrphans(@Nonnull ResourceResolver resolver, @Nonnull String targetRoot,
                                 @Nonnull String deletedPath, @Nonnull String targetReleaseRootPath) throws PersistenceException {
        String originalPath = appendPaths(targetRoot, deletedPath);
        Resource candidate = SlingResourceUtil.getFirstExistingParent(resolver, originalPath);
        while (candidate != null && SlingResourceUtil.isSameOrDescendant(targetReleaseRootPath, candidate.getPath())
                && !ResourceUtil.isNodeType(candidate, ResourceUtil.MIX_VERSIONABLE) && !candidate.hasChildren()) {
            Resource todelete = candidate;
            LOG.info("Remove orphaned node {}", todelete.getPath());
            resolver.delete(todelete);
            candidate = candidate.getParent();
        }
    }


    @Nonnull
    protected Resource getTmpLocation(@Nonnull ResourceResolver resolver, @Nonnull String updateId, boolean create)
            throws RepositoryException, RemotePublicationReceiverException {
        if (StringUtils.isBlank(updateId) || !RemoteReceiverConstants.PATTERN_UPDATEID.matcher(updateId).matches()) {
            throw new IllegalArgumentException("Broken updateId: " + updateId);
        }
        String path = config.tmpDir() + "/" + updateId;
        Resource tmpLocation = resolver.getResource(path);
        if (tmpLocation == null) {
            if (create) {
                tmpLocation = ResourceUtil.getOrCreateResource(resolver, path);
            } else {
                throw new IllegalArgumentException("Unknown updateId " + updateId);
            }
        } else {
            ValueMap vm = tmpLocation.getValueMap();
            String releaseRootPath = vm.get(RemoteReceiverConstants.ATTR_RELEASEROOT_PATH, String.class);
            String originalReleaseChangeId = vm.get(RemoteReceiverConstants.ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, String.class);
            String releaseChangeId = getReleaseChangeId(resolver, releaseRootPath);
            if (releaseChangeId != null && !StringUtils.equals(releaseChangeId, originalReleaseChangeId)) {
                LoggerFactory.getLogger(getClass()).error("Release change id changed since beginning of update: {} to" +
                        " {} . Aborting.", originalReleaseChangeId, releaseChangeId);
                throw new RemotePublicationReceiverException("Release change Id changed since beginning of update - aborting " +
                        "transfer. Retryable.", RemotePublicationReceiverException.RetryAdvice.RETRY_IMMEDIATELY);
            }

        }
        return tmpLocation;
    }

    /** Creates the service resolver used to update the content. */
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    protected String getReleaseChangeId(ResourceResolver resolver, String contentPath) {
        Resource resource = resolver.getResource(contentPath);
        return resource != null ? resource.getValueMap().get(StagingConstants.PROP_REPLICATED_VERSION, String.class) : null;
    }

    /**
     * Adds the sibling orders of the resource and it's parents up to the
     * {@value com.composum.sling.platform.staging.StagingConstants#TYPE_MIX_RELEASE_ROOT} to the data,
     * if there are several.
     *
     * @return the release root
     */
    @Deprecated
    // FIXME(hps,09.12.19) remove this later - probably not needed.
    protected Resource addParentSiblings(@Nonnull ResourceHandle resource, @Nonnull Map<String, List<String>> data) {
        Resource releaseRoot = null;
        if (resource.isOfType(StagingConstants.TYPE_MIX_RELEASE_ROOT)) {
            releaseRoot = resource;
        } else {
            ResourceHandle parent = resource.getParent();
            if (parent != null && parent.isValid()) {
                List<String> childnames = new ArrayList<>();
                for (Resource child : parent.getChildren()) {
                    childnames.add(child.getName());
                }
                if (childnames.size() > 1) {
                    data.put(resource.getPath(), childnames);
                }
                releaseRoot = addParentSiblings(parent, data);
            } else { // we hit / - don't transmit anything here.
                data.clear();
            }
        }
        return releaseRoot;
    }

    @ObjectClassDefinition(
            name = "Composum Platform Remote Publication Receiver Configuration",
            description = "Configures a service that receives release changes from remote system"
    )
    @interface Configuration {

        @AttributeDefinition(
                description = "The general on/off switch for this service."
        )
        boolean enabled() default false;

        @AttributeDefinition(
                description = "Temporary directory to unpack received files."
        )
        String tmpDir() default "/var/composum/tmp/platform/remotereceiver";

        @AttributeDefinition(
                description = "Directory where the content is unpacked. For production use set to /, for testing e.g." +
                        " to /var/composum/tmp to just have a temporary copy of the replicated content to manually " +
                        "inspect there."
        )
        String targetDir() default "/";
    }

}
