package com.composum.platform.replication.remotereceiver;

import com.composum.sling.core.ResourceHandle;
import com.composum.sling.core.servlet.Status;
import com.composum.sling.core.util.ResourceUtil;
import com.composum.sling.platform.staging.StagingConstants;
import com.composum.sling.platform.testing.testutil.AnnotationWithDefaults;
import com.composum.sling.platform.testing.testutil.ErrorCollectorAlwaysPrintingFailures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.resourcebuilder.api.ResourceBuilder;
import org.apache.sling.servlethelpers.MockRequestPathInfo;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletRequest;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.composum.sling.core.util.CoreConstants.TYPE_VERSIONABLE;
import static com.composum.sling.platform.staging.StagingConstants.TYPE_MIX_RELEASE_ROOT;
import static com.composum.sling.platform.staging.StagingConstants.TYPE_MIX_REPLICATEDVERSIONABLE;
import static com.composum.sling.platform.testing.testutil.JcrTestUtils.array;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test {@link RemotePublicationReceiverServlet.ContentStateOperation}. */
public class ContentStateOperationTest {

    @Rule
    public final SlingContext context = new SlingContext(ResourceResolverType.JCR_OAK);

    @Rule
    public final ErrorCollectorAlwaysPrintingFailures ec = new ErrorCollectorAlwaysPrintingFailures();

    protected RemotePublicationReceiverServlet servlet;

    protected RemotePublicationReceiverService service;

    @Before
    public void setup() throws Exception {
        ResourceResolverFactory resolverFactory = mock(ResourceResolverFactory.class);
        ResourceResolver uncloseableResourceResolver = Mockito.spy(context.resourceResolver());
        Mockito.doNothing().when(uncloseableResourceResolver).close();
        when(resolverFactory.getServiceResourceResolver(null)).thenReturn(uncloseableResourceResolver);
        RemotePublicationReceiverService.Configuration config = AnnotationWithDefaults.of(RemotePublicationReceiverService.Configuration.class);

        InputStreamReader cndReader = new InputStreamReader(getClass().getResourceAsStream("/stagingNodetypes.cnd"));
        NodeType[] nodeTypes = CndImporter.registerNodeTypes(cndReader, context.resourceResolver().adaptTo(Session.class));
        assertEquals(5, nodeTypes.length);

        servlet = new RemotePublicationReceiverServlet();
        service = new RemotePublicationReceiverService();
        service.config = config;
        service.resolverFactory = resolverFactory;
        servlet.resolverFactory = resolverFactory;
        servlet.service = service;
    }

    @Test
    public void checkSerialization() throws Exception {
        ResourceBuilder releaseRootBuilder = context.build().resource("/content/some/site", ResourceUtil.PROP_MIXINTYPES,
                array(TYPE_MIX_RELEASE_ROOT)).commit();
        releaseRootBuilder.resource("folder1/page11/jcr:content",
                ResourceUtil.PROP_MIXINTYPES, new String[]{TYPE_VERSIONABLE, TYPE_MIX_REPLICATEDVERSIONABLE},
                StagingConstants.PROP_REPLICATED_VERSION, "f1p11uuid");
        releaseRootBuilder.resource("folder2/page21/jcr:content",
                ResourceUtil.PROP_MIXINTYPES, new String[]{TYPE_VERSIONABLE, TYPE_MIX_REPLICATEDVERSIONABLE},
                StagingConstants.PROP_REPLICATED_VERSION, "f2p21uuid");
        releaseRootBuilder.resource("folder1/page11/sub111/jcr:content",
                ResourceUtil.PROP_MIXINTYPES, new String[]{TYPE_VERSIONABLE, TYPE_MIX_REPLICATEDVERSIONABLE},
                StagingConstants.PROP_REPLICATED_VERSION, "f1p11s111uuid");
        releaseRootBuilder.resource("folder1/page11/sub112/jcr:content",
                ResourceUtil.PROP_MIXINTYPES, new String[]{TYPE_VERSIONABLE, TYPE_MIX_REPLICATEDVERSIONABLE},
                StagingConstants.PROP_REPLICATED_VERSION, "f1p11s212uuid");
        Resource releaseRoot = releaseRootBuilder.commit().getCurrentParent();
        ResourceHandle page11 = ResourceHandle.use(releaseRoot.getChild("folder1/page11"));

        MockSlingHttpServletRequest request = context.request();
        MockSlingHttpServletResponse response = context.response();

        RemotePublicationReceiverServlet.ContentStateOperation cso = servlet.new ContentStateOperation();
        MockRequestPathInfo rpi = (MockRequestPathInfo) request.getRequestPathInfo();
        rpi.setSuffix("/content/some/site/folder1");
        cso.doIt(request, response, page11);
        ec.checkThat(response.getStatus(), is(200));
        ec.checkThat(response.getOutputAsString(),
                is("{\"versionables\":[" +
                        "{\"path\":\"/content/some/site/folder1/page11/jcr:content\",\"version\":\"f1p11uuid\"}," +
                        "{\"path\":\"/content/some/site/folder1/page11/sub111/jcr:content\",\"version\":\"f1p11s111uuid\"}," +
                        "{\"path\":\"/content/some/site/folder1/page11/sub112/jcr:content\",\"version\":\"f1p11s212uuid\"}]," +
                        "\"status\":200,\"success\":true,\"warning\":false}"));

        Gson gson = new GsonBuilder().create();
        TestStatusWithAttributes status = gson.fromJson(response.getOutputAsString(), TestStatusWithAttributes.class);
        ec.checkThat(status.getStatus(), is(200));
        ec.checkThat(status.versionables.size(), is(3));
        ec.checkThat(status.versionables.get(1).get("version"), is("f1p11s111uuid"));
    }

    private static class TestStatusWithAttributes extends Status {

        public TestStatusWithAttributes(@Nonnull SlingHttpServletRequest request, @Nonnull SlingHttpServletResponse response) {
            super(request, response);
        }

        List<Map<String, Object>> versionables;
    }

}
