<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <input type="hidden" name="sling:resourceType" value="composum/platform/replication/inplace">
    <input type="hidden" name="jcr:mixinTypes" value="vlt:FullCoverage">
</cpn:component>
