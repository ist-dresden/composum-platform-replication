package com.composum.platform.replication.remote;

import com.composum.platform.commons.json.JSonOnTheFlyCollectionAdapter;
import com.composum.sling.core.util.ResourceUtil;
import com.google.gson.annotations.JsonAdapter;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.sling.api.resource.Resource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Models a tree of versionables. Serves dual purpose - to scan a resource tree and write the found versionables to
 * JSON, and to read that back and compare it to the existing data. This allows defining one class to serialize and
 * serialize the result.
 */
@JsonAdapter(JSonOnTheFlyCollectionAdapter.class)
// FIXME(hps,11.12.19) might or might not be used.
public class VersionableTree implements Iterable<VersionableInfo>, Consumer<VersionableInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(VersionableTree.class);

    protected Resource topResource;

    /** For construction from JSON. */
    public VersionableTree() {
        // empty
    }

    /** For generating JSON: traverses tree through topResource. */
    public VersionableTree(Resource topResource) {
        this.topResource = topResource;
    }

    @NotNull
    @Override
    public Iterator<VersionableInfo> iterator() {
        if (topResource == null) { return Collections.emptyIterator(); }
        Iterator nodes = IteratorUtils.objectGraphIterator(topResource, this::childrenUntilVersionable);
        Iterator<VersionableInfo> versionableInfo =
                IteratorUtils.transformedIterator((Iterator<Resource>) nodes, VersionableInfo::of);
        Iterator<VersionableInfo> filtered = IteratorUtils.filteredIterator(versionableInfo, Objects::nonNull);
        return filtered;
    }

    protected Object childrenUntilVersionable(Object object) {
        Resource resource = (Resource) object;
        if (resource.getName().equals(ResourceUtil.CONTENT_NODE) || ResourceUtil.isNodeType(resource,
                ResourceUtil.TYPE_VERSIONABLE)) {
            return resource;
        }
        return resource.getChildren().iterator();
    }

    @Override
    public void accept(VersionableInfo versionableInfo) {
// FIXME OUCH we need access to the resolver, anyway, so this doesn't work!
        LOG.error("VersionableTree.accept");
        if (0 == 0) { throw new UnsupportedOperationException("Not implemented yet: VersionableTree.accept"); }
        // FIXME hps 10.12.19 implement VersionableTree.accept

    }

}
