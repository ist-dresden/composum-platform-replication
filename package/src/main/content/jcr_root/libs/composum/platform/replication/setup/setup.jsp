<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationSetup" scope="request">
    <div data-path="${model.path}" class="composum-platform-replication-setup">
        <sling:call script="content.jsp"/>
    </div>
</cpn:component>
