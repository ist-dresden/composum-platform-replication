package com.composum.platform.replication.remotereceiver;

import com.composum.platform.commons.json.JsonArrayAsIterable;
import com.composum.platform.replication.json.ChildrenOrderInfo;
import com.composum.platform.replication.json.NodeAttributeComparisonInfo;
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
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_RELEASEROOT_PATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_TOP_CONTENTPATH;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.ATTR_UPDATEDPATHS;
import static com.composum.platform.replication.remotereceiver.RemoteReceiverConstants.PATTERN_UPDATEID;
import static com.composum.sling.core.util.SlingResourceUtil.appendPaths;
import static com.composum.sling.platform.staging.StagingConstants.PROP_LAST_REPLICATION_DATE;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * Interface for service that implements the functions behind the {@link RemotePublicationReceiverServlet}.
 */
@Component(
        service = RemotePublicationReceiver.class,
        property = {Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Receiver Service"},
        configurationPolicy = ConfigurationPolicy.REQUIRE
)
@Designate(ocd = RemotePublicationReceiverService.Configuration.class)
public class RemotePublicationReceiverService implements RemotePublicationReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverService.class);

    protected volatile Configuration config;

    @Reference
    protected ResourceResolverFactory resolverFactory;

    /**
     * Random number generator for creating unique ids etc.
     */
    protected final Random random;

    /**
     * Debugging aid - if set to true, the temporary directory will not be deleted.
     */
    protected boolean nodelete = false;

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
    public UpdateInfo startUpdate(@Nonnull String releaseRootPath, @Nonnull String contentPath)
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

            fillUpdateInfo(updateInfo, resolver, releaseRootPath);
            if (StringUtils.isNotBlank(updateInfo.originalPublisherReleaseChangeId)) {
                vm.put(ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, updateInfo.originalPublisherReleaseChangeId);
            }
            resolver.commit();
            return updateInfo;
        }
    }

    protected void fillUpdateInfo(UpdateInfo updateInfo, ResourceResolver resolver, String releaseRootPath) {
        Resource releaseRoot = getReleaseRoot(resolver, releaseRootPath);
        updateInfo.originalPublisherReleaseChangeId = getReleaseChangeId(resolver, releaseRootPath);
        Calendar lastReplicationDate = releaseRoot != null ?
                releaseRoot.getValueMap().get(PROP_LAST_REPLICATION_DATE, Calendar.class)
                : null;
        updateInfo.lastReplication = lastReplicationDate != null ? lastReplicationDate.getTimeInMillis() : null;
    }

    @Nullable
    protected Resource getReleaseRoot(ResourceResolver resolver, String releaseRootPath) {
        return resolver.getResource(appendPaths(getTargetDir(), releaseRootPath));
    }

    @Nullable
    @Override
    public UpdateInfo releaseInfo(@Nullable String releaseRootPath) throws LoginException {
        UpdateInfo result = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("ReleaseInfo called for {}", releaseRootPath);
        }
        if (StringUtils.isNotBlank(releaseRootPath)) {
            try (ResourceResolver resolver = makeResolver()) {
                Resource releaseRoot = getReleaseRoot(resolver, releaseRootPath);
                if (releaseRoot != null) {
                    result = new UpdateInfo();
                    fillUpdateInfo(result, resolver, releaseRootPath);
                }
            }
        }
        return result;
    }

    @Nonnull
    @Override
    public List<String> compareContent(String contentPath, @Nullable String updateId, @Nonnull BufferedReader json)
            throws LoginException, RemotePublicationReceiverException, RepositoryException, IOException {
        LOG.info("Compare content {} - {}", updateId, contentPath);
        try (ResourceResolver resolver = makeResolver(); Reader ignored = json) {
            String path = contentPath;
            if (StringUtils.isNotBlank(updateId)) {
                Resource tmpLocation = getTmpLocation(resolver, updateId, false);
                String releaseRootPath = tmpLocation.getValueMap().get(ATTR_RELEASEROOT_PATH, String.class);
                if (StringUtils.isBlank(contentPath)) {
                    path = tmpLocation.getValueMap().get(ATTR_TOP_CONTENTPATH, String.class);
                } else if (!StringUtils.startsWith(path, releaseRootPath)) { // safety-check - that'd be a bug.
                    throw new IllegalArgumentException("contentpath " + path + " is not subpath of release root " + releaseRootPath);
                }
            }

            VersionableTree.VersionableTreeDeserializer factory =
                    new VersionableTree.VersionableTreeDeserializer(config.targetDir(), resolver, path);
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
    public void pathUpload(@Nullable String updateId, @Nonnull String packageRootPath, @Nonnull InputStream inputStream)
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
    public void commit(@Nonnull String updateId, @Nonnull Set<String> deletedPaths,
                       @Nonnull JsonArrayAsIterable<ChildrenOrderInfo> childOrderings, String newReleaseChangeId)
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
                deletePath(resolver, tmpLocation, targetRoot, deletedPath);
            }

            @Nonnull String targetReleaseRootPath = appendPaths(targetRoot, releaseRootPath);
            Resource targetReleaseRoot = ResourceUtil.getOrCreateResource(resolver, targetReleaseRootPath);

            for (String updatedPath : updatedPaths) {
                if (!SlingResourceUtil.isSameOrDescendant(topContentPath, updatedPath)) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + topContentPath + " : " + updatedPath);
                }
                if (!deletedPaths.contains(updatedPath)) {
                    // if it's deleted we needed to transfer a package for it, anyway, to update it's parents
                    // attributes. So it's in updatedPath, too, but doesn't need to be moved.
                    moveVersionable(resolver, tmpLocation, updatedPath, targetRoot);
                }
            }

            for (String deletedPath : deletedPaths) {
                removeOrphans(resolver, targetRoot, deletedPath, targetReleaseRootPath);
            }

            for (ChildrenOrderInfo childrenOrderInfo : childOrderings) {
                if (!SlingResourceUtil.isSameOrDescendant(releaseRootPath, childrenOrderInfo.getPath())) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + releaseRootPath + " : " + childrenOrderInfo);
                }
                String targetPath = appendPaths(targetRoot, childrenOrderInfo.getPath());
                Resource resource = resolver.getResource(targetPath);
                if (resource != null) {
                    adjustChildrenOrder(resource, childrenOrderInfo.getChildNames());
                } else { // bug or concurrent modification
                    LOG.error("Resource for childorder doesn't exist: {}", targetPath);
                }
            }
            LOG.debug("Number of child orderings read for {} was {}", topContentPath, childOrderings.getNumberRead());

            ModifiableValueMap releaseRootVm = targetReleaseRoot.adaptTo(ModifiableValueMap.class);
            releaseRootVm.put(StagingConstants.PROP_LAST_REPLICATION_DATE, Calendar.getInstance());
            releaseRootVm.put(StagingConstants.PROP_CHANGE_NUMBER, newReleaseChangeId);
            if (!nodelete) {
                resolver.delete(tmpLocation);
            }
            resolver.commit();
        }
    }

    protected void adjustChildrenOrder(@Nonnull Resource resource, @Nonnull List<String> childNames) throws RepositoryException {
        LOG.debug("Checking children order for {}", SlingResourceUtil.getPath(resource));
        List<String> currentChildNames = StreamSupport.stream(resource.getChildren().spliterator(), false)
                .map(Resource::getName)
                .collect(Collectors.toList());
        if (!childNames.equals(currentChildNames)) {
            Node node = requireNonNull(resource.adaptTo(Node.class));
            try {
                for (String childName : childNames) {
                    try {
                        node.orderBefore(childName, null); // move to end of list
                    } catch (RepositoryException | RuntimeException e) {
                        LOG.error("Trouble reordering {} : {} from {}", SlingResourceUtil.getPath(resource),
                                childName, childNames);
                        throw e;
                    }
                }

                currentChildNames = StreamSupport.stream(resource.getChildren().spliterator(), false)
                        .map(Resource::getName)
                        .collect(Collectors.toList());
                if (!childNames.equals(currentChildNames)) { // Bug or concurrent modification at source side
                    LOG.error("Reordering failed for {} : {} but still got {}", resource.getPath(), childNames,
                            currentChildNames);
                }
            } catch (UnsupportedRepositoryOperationException e) { // should be impossible.
                LOG.error("Bug: Child nodes not orderable for {} type {}", resource.getPath(),
                        resource.getValueMap().get(ResourceUtil.PROP_PRIMARY_TYPE, String.class));
            }
        }
    }

    @Override
    public void abort(@Nullable String updateId) throws LoginException, RemotePublicationReceiverException,
            RepositoryException, PersistenceException {
        LOG.info("Abort called for {}", updateId);
        if (nodelete) {
            return;
        }
        try (ResourceResolver resolver = makeResolver()) {
            Resource tmpLocation = getTmpLocation(resolver, updateId, false);
            resolver.delete(tmpLocation);
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
        Session session = Objects.requireNonNull(destinationParent.getResourceResolver().adaptTo(Session.class));

        if (destination != null) {
            // can't replace the node since OAK wrongly thinks we changed protected attributes
            // see com.composum.platform.replication.remote.ReplacementStrategyExplorationTest.bugWithReplace
            // we copy the attributes and move the children, instead, so protected attributes stay the same.
            synchronizer.updateAttributes(ResourceHandle.use(source), ResourceHandle.use(destination), ImmutableBiMap.of());
            for (Resource previousChild : destination.getChildren()) {
                resolver.delete(previousChild);
            }
            for (Resource child : source.getChildren()) {
                session.move(child.getPath(), destination.getPath() + "/" + child.getName());
                // avoid resolver.move(child.getPath(), destination.getPath()); because brittle
            }
        } else {
            Resource sourceParent = source.getParent();
            // use JCR move because of OAK-bugs: this is sometimes treated as copy and delete, which even fails
            // should be resolver.move(source.getPath(), destinationParent.getPath());
            session.move(source.getPath(), destinationParent.getPath() + "/" + nodename);
            if (ResourceUtil.isFile(sourceParent) && !sourceParent.hasChildren()) {
                resolver.delete(sourceParent); // otherwise tmpdir would be inconsistent.
            }
        }

        LOG.info("Moved {} to {}", SlingResourceUtil.getPath(source),
                SlingResourceUtil.getPath(destinationParent) + "/" + nodename);
    }

    protected void deletePath(@Nonnull ResourceResolver resolver, @Nonnull Resource tmpLocation,
                              @Nonnull String targetRoot, @Nonnull String deletedPath) throws PersistenceException, RepositoryException {
        NodeTreeSynchronizer synchronizer = new NodeTreeSynchronizer();
        Resource source = tmpLocation;
        Resource destination = requireNonNull(resolver.getResource(targetRoot), targetRoot);
        for (String pathsegment : StringUtils.removeStart(ResourceUtil.getParent(deletedPath), "/").split("/")) {
            source = source.getChild(pathsegment);
            destination = destination.getChild(pathsegment);
            if (source == null || destination == null) {
                break;
            }
            synchronizer.updateAttributes(ResourceHandle.use(source), ResourceHandle.use(destination), ImmutableBiMap.of());
        }

        Resource deletedResource = resolver.getResource(appendPaths(targetRoot, deletedPath));
        if (deletedResource != null) {
            LOG.info("Deleting {}", deletedPath);
            resolver.delete(deletedResource);
        } else { // some problem with the algorithm!
            LOG.warn("Path to delete unexpectedly not present in content: {}", deletedPath);
        }
    }

    /**
     * Removes parent nodes of the deleted nodes that do not have any (versionable) children now.
     */
    protected void removeOrphans(@Nonnull ResourceResolver resolver, @Nonnull String targetRoot,
                                 @Nonnull String deletedPath, @Nonnull String targetReleaseRootPath) throws PersistenceException {
        String originalPath = appendPaths(targetRoot, deletedPath);
        Resource candidate = SlingResourceUtil.getFirstExistingParent(resolver, originalPath);
        while (candidate != null && SlingResourceUtil.isSameOrDescendant(targetReleaseRootPath, candidate.getPath())
                && !ResourceUtil.isNodeType(candidate, ResourceUtil.MIX_VERSIONABLE) && !candidate.hasChildren()) {
            Resource todelete = candidate;
            candidate = candidate.getParent();
            LOG.info("Remove orphaned node {}", todelete.getPath());
            resolver.delete(todelete);
        }
    }


    @Nonnull
    protected Resource getTmpLocation(@Nonnull ResourceResolver resolver, @Nullable String updateId, boolean create)
            throws RepositoryException, RemotePublicationReceiverException {
        cleanup(resolver);

        if (StringUtils.isBlank(updateId) || !RemoteReceiverConstants.PATTERN_UPDATEID.matcher(updateId).matches()) {
            throw new IllegalArgumentException("Broken updateId: " + updateId);
        }
        String path = config.tmpDir() + "/" + updateId;
        Resource tmpLocation = resolver.getResource(path);
        if (tmpLocation == null) {
            if (create) {
                tmpLocation = ResourceUtil.getOrCreateResource(resolver, path);
                tmpLocation.adaptTo(ModifiableValueMap.class).put(ResourceUtil.PROP_MIXINTYPES,
                        new String[]{ResourceUtil.MIX_CREATED, ResourceUtil.MIX_LAST_MODIFIED});
            } else {
                throw new IllegalArgumentException("Unknown updateId " + updateId);
            }
        } else {
            ModifiableValueMap vm = tmpLocation.adaptTo(ModifiableValueMap.class);
            vm.put(ResourceUtil.PROP_LAST_MODIFIED, new Date());
            String releaseRootPath = vm.get(RemoteReceiverConstants.ATTR_RELEASEROOT_PATH, String.class);
            String originalReleaseChangeId = vm.get(ATTR_OLDPUBLISHERCONTENT_RELEASECHANGEID, String.class);
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

    /**
     * Removes old temporary directories to make space. That shouldn't be necessary except in serious failure cases
     * like interrupted connection or crashed servers during a replication.
     * We assume a directory last touched more than a day ago needs
     * to be removed - the {@link ResourceUtil#PROP_LAST_MODIFIED} is changed on each access with {@link #getTmpLocation(ResourceResolver, String, boolean)}.
     */
    protected void cleanup(ResourceResolver resolver) {
        int cleanupDays = config.cleanupTmpdirDays();
        if (cleanupDays < 1 || StringUtils.length(config.tmpDir()) < 4) {
            return;
        }
        Resource tmpDir = resolver.getResource(config.tmpDir());
        if (tmpDir == null) { // impossible
            LOG.warn("Can't find temporary directory for cleanup: {}", config.tmpDir());
            return;
        }
        long expireTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(cleanupDays);
        for (Resource child : tmpDir.getChildren()) {
            if (PATTERN_UPDATEID.matcher(child.getName()).matches()) {
                Date modificationDate = child.getValueMap().get(ResourceUtil.PROP_LAST_MODIFIED, Date.class);
                if (modificationDate == null) {
                    modificationDate = child.getValueMap().get(ResourceUtil.PROP_CREATED, Date.class);
                }
                if (modificationDate != null && modificationDate.getTime() < expireTime) {
                    LOG.error("Cleanup: needing to delete temporary directory not touched for {} days: {}",
                            cleanupDays, child.getPath());
                    try {
                        resolver.delete(child);
                    } catch (PersistenceException e) {
                        LOG.error("Error deleting " + child.getPath(), e);
                    }
                }
            }
        }
    }

    @Override
    @Nonnull
    public List<String> compareChildorderings(@Nullable String releaseRootPath,
                                              @Nonnull JsonArrayAsIterable<ChildrenOrderInfo> childOrderings)
            throws LoginException, RemotePublicationReceiverException {
        LOG.info("Compare child orderings for {}", releaseRootPath);
        List<String> result = new ArrayList<>();
        try (ResourceResolver resolver = makeResolver()) {
            String targetRoot = requireNonNull(config.targetDir());
            Resource releaseRoot = StringUtils.isNotBlank(releaseRootPath) ? resolver.getResource(appendPaths(targetRoot, releaseRootPath)) : null;
            if (releaseRoot == null) {
                LOG.error("Release root for {} not found below {}", releaseRootPath, targetRoot);
                throw new RemotePublicationReceiverException("Release root not found",
                        RemotePublicationReceiverException.RetryAdvice.NO_AUTOMATIC_RETRY);
            }
            for (ChildrenOrderInfo childrenOrderInfo : childOrderings) {
                if (!SlingResourceUtil.isSameOrDescendant(releaseRootPath, childrenOrderInfo.getPath())) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + releaseRootPath + " : " + childrenOrderInfo);
                }
                String targetPath = appendPaths(targetRoot, childrenOrderInfo.getPath());
                Resource resource = resolver.getResource(targetPath);
                if (resource != null) {
                    if (!equalChildrenOrder(resource, childrenOrderInfo.getChildNames())) {
                        result.add(childrenOrderInfo.getPath());
                    }
                } else {
                    LOG.debug("resource for compareChildorderings not found: {}", targetPath);
                    result.add(childrenOrderInfo.getPath());
                }
            }
        }
        LOG.debug("Number of child orderings read for {} was {}", releaseRootPath, childOrderings.getNumberRead());
        return result;
    }

    protected boolean equalChildrenOrder(@Nonnull Resource resource, @Nonnull List<String> childNames) {
        LOG.debug("compare: {}, {}", SlingResourceUtil.getPath(resource), childNames);
        List<String> currentChildNames = StreamSupport.stream(resource.getChildren().spliterator(), false)
                .map(Resource::getName)
                .collect(Collectors.toList());
        boolean result = currentChildNames.equals(childNames);
        if (!result) {
            LOG.debug("different children order at {}", resource.getPath());
        }
        return result;
    }

    @Override
    @Nonnull
    public List<String> compareAttributes(@Nullable String releaseRootPath,
                                          @Nonnull JsonArrayAsIterable<NodeAttributeComparisonInfo> attributeInfos)
            throws LoginException, RemotePublicationReceiverException {
        LOG.info("Compare parent attributes for {}", releaseRootPath);
        List<String> result = new ArrayList<>();
        try (ResourceResolver resolver = makeResolver()) {
            String targetRoot = requireNonNull(config.targetDir());
            Resource releaseRoot = StringUtils.isNotBlank(releaseRootPath) ? resolver.getResource(appendPaths(targetRoot, releaseRootPath)) : null;
            if (releaseRoot == null) {
                LOG.error("Release root for {} not found below {}", releaseRootPath, targetRoot);
                throw new RemotePublicationReceiverException("Release root not found",
                        RemotePublicationReceiverException.RetryAdvice.NO_AUTOMATIC_RETRY);
            }
            for (NodeAttributeComparisonInfo attributeInfo : attributeInfos) {
                if (!SlingResourceUtil.isSameOrDescendant(releaseRootPath, attributeInfo.path)) { // safety check - Bug!
                    throw new IllegalArgumentException("Not subpath of " + releaseRootPath + " : " + attributeInfo);
                }
                String targetPath = appendPaths(targetRoot, attributeInfo.path);
                Resource resource = resolver.getResource(targetPath);
                if (resource != null) {
                    NodeAttributeComparisonInfo ourAttributeInfo =
                            NodeAttributeComparisonInfo.of(resource, config.targetDir());
                    if (!attributeInfo.equals(ourAttributeInfo)) {
                        result.add(attributeInfo.path);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Different attributes for {} : {}", attributeInfo.path,
                                    attributeInfo.difference(ourAttributeInfo));
                        }
                    }
                } else {
                    LOG.debug("resource for compareParentPaths not found: {}", targetPath);
                    result.add(attributeInfo.path);
                }
            }
        }
        LOG.debug("Number of parent attribute infos read for {} was {}", releaseRootPath,
                attributeInfos.getNumberRead());
        return result;

    }


    /**
     * Creates the service resolver used to update the content.
     */
    @Nonnull
    protected ResourceResolver makeResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(null);
    }

    @Nullable
    protected String getReleaseChangeId(@Nonnull ResourceResolver resolver, @Nonnull String releaseRootPath) {
        Resource releaseRoot = getReleaseRoot(resolver, releaseRootPath);
        return releaseRoot != null ? releaseRoot.getValueMap().get(StagingConstants.PROP_CHANGE_NUMBER, String.class) : null;
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
        String tmpDir() default "/tmp/composum/platform/remotereceiver";

        @AttributeDefinition(
                description = "Directory where the content is unpacked. For production use set to /, for testing e.g." +
                        " to /tmp/composum/platform/replicationtest to just have a temporary copy of the replicated content to manually " +
                        "inspect there."
        )
        String targetDir() default "/";

        @AttributeDefinition(
                description = "Automatic removal of stale temporary directories used for replication after this many " +
                        "days. Normally they are removed immediately after completion / abort."
        )
        int cleanupTmpdirDays() default 1;
    }

}
