<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <div class="composum-platform-replication-config-node_general">
        <span class="badge">${cpn:text(model.property.releaseMark)}</span>
        <span class="type key"> - type :</span>
        <span class="type value">${cpn:text(model.replicationType.title)}</span>
        <span class="path key"> - path :</span>
        <span class="path value">${cpn:text(model.contentPath)}</span>
    </div>
</cpn:component>
