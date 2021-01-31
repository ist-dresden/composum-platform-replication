<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <sling:call script="title.jsp"/>
    <sling:call script="general.jsp"/>
    <div class="composum-platform-replication-node_settings">
        <span class="key">${cpn:i18n(slingRequest,'Target URL')} :</span>
        <span class="value">${cpn:text(model.property.targetUrl)}</span>
    </div>
</cpn:component>
