<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <h4 class="composum-platform-replication-config-node_title">
        <i class="switch enabled fa fa-toggle-${model.property.enabled?'on':'off'}"></i>
            ${cpn:text(model.title)}
    </h4>
</cpn:component>
