<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <div class="composum-platform-replication-node_general">
        <span class="badge">${cpn:i18n(slingRequest,model.property.stage)}</span>
        <span class="type key"> - ${cpn:i18n(slingRequest,'Type')} :</span>
        <span class="type value">${cpn:i18n(slingRequest,model.replicationType.title)}</span>
        <span class="path key"> - ${cpn:i18n(slingRequest,'Path')} :</span>
        <span class="path value">${cpn:text(model.sourcePath)}</span>
    </div>
</cpn:component>
