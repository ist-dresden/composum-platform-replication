package com.composum.replication.remote;

import com.composum.sling.core.servlet.AbstractServiceServlet;
import com.composum.sling.core.servlet.ServletOperationSet;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.servlets.HttpConstants;
import org.apache.sling.api.servlets.ServletResolverConstants;
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

import javax.servlet.Servlet;
import javax.servlet.ServletException;

@Component(service = Servlet.class,
        property = {
                Constants.SERVICE_DESCRIPTION + "=Composum Platform Remote Publication Receiver Servlet",
                ServletResolverConstants.SLING_SERVLET_PATHS + "=/bin/cpm/platform/replication/publishreceiver",
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_GET,
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_POST,
                ServletResolverConstants.SLING_SERVLET_METHODS + "=" + HttpConstants.METHOD_PUT
        })
@Designate(ocd = RemotePublicationReceiverServlet.Configuration.class)
public class RemotePublicationReceiverServlet extends AbstractServiceServlet {

    private static final Logger LOG = LoggerFactory.getLogger(RemotePublicationReceiverServlet.class);

    /** Parameter for a changed path. */
    public static final String PARAM_PATH = "path";

    /** Parameter for a changed path. */
    public static final String PARAM_DELETED_PATH = "deletedpath";

    /**
     * Mandatory parameter that points to the release root. Should be deliberately used as last part in the request,
     * to easily ensure that the whole request was transmitted.
     */
    public static final String PARAM_RELEASEROOT = "releaseRoot";

    protected volatile Configuration config;

    public enum Extension {zip, json}

    public enum Operation {replaceContent, contentstate, startupdate, commitupdate, pathupload}

    protected final ServletOperationSet<Extension, Operation> operations = new ServletOperationSet<>(Extension.json);

    @Reference
    protected ResourceResolverFactory resolverFactory;

    @Override
    protected boolean isEnabled() {
        Configuration theconfig = this.config;
        return theconfig != null && theconfig.enabled();
    }

    @Override
    protected ServletOperationSet getOperations() {
        return operations;
    }

    @Override
    public void init() throws ServletException {
        super.init();

        operations.setOperation(ServletOperationSet.Method.POST, Extension.zip, Operation.replaceContent,
                new ReplaceContentOperation(this::getConfig, resolverFactory));
        operations.setOperation(ServletOperationSet.Method.GET, Extension.json, Operation.contentstate,
                new ContentStateOperation(this::getConfig, resolverFactory));
    }

    @Activate
    @Modified
    protected void activate(Configuration config) {
        this.config = config;
    }

    @Deactivate
    protected void deactivate() {
        this.config = null;
    }

    protected Configuration getConfig() {
        return config;
    }

    @ObjectClassDefinition(
            name = "Composum Platform Remote Publication Receiver Configuration",
            description = "Configures a service that receives release changes from remote system"
    )
    public static @interface Configuration {

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
