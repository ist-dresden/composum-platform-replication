<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationSetup" scope="request">
    <form class="composum-platform-replication-config-setup_form widget-form">
        <ul class="composum-commons-form-tab-nav nav nav-tabs" role="tablist">
        </ul>
        <div class="composum-platform-replication-config-setup_panels tab-content composum-commons-form-tabbed">
            <div id="${model.domId}_path" data-key="path"
                 data-label="${cpn:i18n(slingRequest,'by Path')}"
                 class="composum-commons-form-tab-panel tab-pane" role="tabpanel">
                <sling:call script="bypath.jsp"/>
            </div>
            <div id="${model.domId}_type" data-key="type"
                 data-label="${cpn:i18n(slingRequest,'by Type')}"
                 class="composum-commons-form-tab-panel tab-pane" role="tabpanel">
                <sling:call script="bytype.jsp"/>
            </div>
        </div>
    </form>
</cpn:component>
