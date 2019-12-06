package com.composum.replication.remote;

import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.testing.testutil.JcrTestUtils;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.resourcebuilder.api.ResourceBuilder;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Workspace;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Reproduces weird ConstraintViolationExceptions when moving around versionables. */
public class ReplacementStrategyExploration {

    @Rule
    public final SlingContext context = new SlingContext(ResourceResolverType.JCR_OAK);

    private Resource first;
    private Resource firstParent;
    private Resource second;
    private Resource secondParent;
    private Resource folder;
    private ResourceResolver resolver;
    private Session session;

    @Before
    public void setup() throws PersistenceException {
        ResourceBuilder builder = context.build().resource("/content/somewhere");
        first = builder.resource("1/node/", JCR_PRIMARYTYPE, NT_FILE)
                .resource(JCR_CONTENT, JCR_PRIMARYTYPE, NT_RESOURCE,
                        JCR_MIXINTYPES, new String[]{MIX_VERSIONABLE},
                        ResourceUtil.PROP_DATA, "data1").getCurrentParent();
        firstParent = first.getParent();
        second = builder.resource("2/node/", JCR_PRIMARYTYPE, NT_FILE)
                .resource(JCR_CONTENT, JCR_PRIMARYTYPE, NT_RESOURCE,
                        JCR_MIXINTYPES, new String[]{MIX_VERSIONABLE},
                        ResourceUtil.PROP_DATA, "data2").getCurrentParent();
        secondParent = second.getParent();
        folder = builder.resource("folder").getCurrentParent();
        builder.commit();
        resolver = context.resourceResolver();
        resolver.commit();
        session = resolver.adaptTo(Session.class);
    }

    @After
    public void teardown() throws PersistenceException {
        JcrTestUtils.printResourceRecursivelyAsJson(resolver, "/content");
    }

    @Test
    public void onlyPrint() throws PersistenceException {
        // JcrTestUtils.printResourceRecursivelyAsJson(resolver, "/content");
    }

    @Test
    public void move() throws PersistenceException {
        resolver.move(first.getParent().getPath(), folder.getPath());
        resolver.commit();
    }

    /** Copying versionables isn't possible. :-( Not quite clear if that's a bug or feature. */
    @Test(expected = PersistenceException.class)
    public void copyDoesNotWork() throws PersistenceException {
        resolver.copy(first.getParent().getPath(), folder.getPath());
        resolver.commit();
    }

    /**
     * This fails because the jcr:content node is now missing from first/ . So we might need to move or delete the
     * original parent nodes.
     */
    @Test(expected = PersistenceException.class)
    public void moveJcrContentFails() throws PersistenceException {
        resolver.move(first.getPath(), folder.getPath());
        resolver.commit();
    }

    /** This doesn't fail since we delete the parent, so that it doesn't cause a constraint violation anymore. */
    @Test
    public void moveJcrContentFailsNotIfOriginDeleted() throws PersistenceException {
        resolver.move(first.getPath(), folder.getPath());
        resolver.delete(firstParent);
        resolver.commit();
    }

    /**
     * This should actually work, but it complains about changing a protected property:
     * org.apache.jackrabbit.oak.api.CommitFailedException: OakConstraint0100: Property is protected: jcr:versionHistory
     * It seems oak thinks here the moved property from the second node is a property change. :-(
     */
    @Test(expected = PersistenceException.class)
    public void bugWithReplace() throws PersistenceException {
        resolver.delete(second);
        resolver.move(first.getPath(), secondParent.getPath());
        resolver.delete(firstParent);
        resolver.commit();
    }

    /**
     * We can avoid the bug by moving the whole tree to somewhere else, such that the verification now thinks it is a
     * different node, then quickly move it back. That does, however, leave a small gap where the content tree is
     * somewhere else, so it's a bad workaround.
     */
    @Test
    public void bugWithReplaceBadWorkaround() throws PersistenceException, RepositoryException {
        resolver.delete(second);
        resolver.move(first.getPath(), secondParent.getPath());
        resolver.delete(firstParent);
        // trick verification to think it's a different node
        session.move("/content/somewhere", "/content/somewhere-tmp");
        resolver.commit();
        session.move("/content/somewhere-tmp", "/content/somewhere"); // move it back
        resolver.commit();
    }


    /**
     * This should actually work, but it complains about changing a protected property:
     * org.apache.jackrabbit.oak.api.CommitFailedException: OakConstraint0100: Property is protected: jcr:versionHistory
     * It seems oak thinks here the moved property from the second node is a property change. :-(
     */
    @Test(expected = PersistenceException.class)
    public void bugWithReplaceThroughSession() throws PersistenceException, RepositoryException {
        try {
            String secondPath = second.getPath();
            resolver.delete(second);
            session.move(first.getPath(), secondPath);
            resolver.delete(firstParent);
            resolver.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    /**
     * This should actually work, but it complains about changing a protected property:
     * org.apache.jackrabbit.oak.api.CommitFailedException: OakConstraint0100: Property is protected: jcr:versionHistory
     * It seems oak thinks here the moved property from the second node is a property change. :-(
     */
    @Test(expected = PersistenceException.class)
    public void bugReplaceParents() throws PersistenceException {
        String destination = secondParent.getParent().getPath();
        resolver.delete(secondParent);
        // resolver.commit(); // if we introduce this, it works, but that would be an inconsistend intermediate state
        resolver.move(firstParent.getPath(), destination);
        resolver.commit();
    }

    @Test
    public void replaceByParentsWorks() throws PersistenceException {
        String destination = secondParent.getParent().getPath();
        resolver.delete(secondParent);
        resolver.commit(); // this works around the bug, but that's not something we can use.
        resolver.move(firstParent.getPath(), destination);
        resolver.commit();
    }

    /** Sharing would have provided a workaround, but unfortunately isn't implemented. :-( */
    @Test
    public void checkSharedNodes() throws RepositoryException {
        Resource shared = context.build().resource("/content/shared/test", JCR_PRIMARYTYPE, NT_UNSTRUCTURED,
                JCR_MIXINTYPES, new String[]{"mix:shareable"}).commit().getCurrentParent();
        Node node = shared.adaptTo(Node.class);
        NodeIterator sharedSet = node.getSharedSet();
        assertThat(new IteratorIterable<Object>(sharedSet), Matchers.<Object>iterableWithSize(1));
        Workspace workspace = session.getWorkspace();
        try {
            workspace.clone(null, "/content/shared/test", "/content/shared/test2", false);
            fail("Oh - sharing is supported now! Possibly rethink implementation.");
        } catch (UnsupportedRepositoryOperationException e) {
            // expected. :-(
        }
    }

}
