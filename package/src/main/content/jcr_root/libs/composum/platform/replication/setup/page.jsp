<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<html class="composum-platform-replication-setup_page" data-context-path="${slingRequest.contextPath}">
<head>
    <cpn:clientlib type="css" category="composum.platform.replication"/>
</head>
<body class="composum-platform-replication-setup_page-body">
<sling:include path="${not empty slingRequest.requestPathInfo.suffix?cpn:filter(slingRequest.requestPathInfo.suffix):'.'}"
               resourceType="composum/platform/replication/setup" replaceSelectors=""/>
<cpn:clientlib type="js" category="composum.platform.replication"/>
<script>
    $(document).ready(function () {
        CPM.core.getWidget(document, '.composum-platform-replication-setup', CPM.platform.replication.ConfigSetup);
    });
</script>
</body>
</html>
