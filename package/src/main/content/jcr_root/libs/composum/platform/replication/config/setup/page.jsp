<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<html class="composum-platform-replication-config-setup_page" data-context-path="${slingRequest.contextPath}">
<head>
    <cpn:clientlib type="css" category="composum.platform.replication"/>
</head>
<body class="composum-platform-replication-config-setup_page-body">
<sling:include replaceSelectors=""/>
<cpn:clientlib type="js" category="composum.platform.replication"/>
<script>
    $(document).ready(function () {
        CPM.core.getWidget(document, '.composum-platform-replication-config-setup', CPM.platform.replication.config.ConfigSetup);
    });
</script>
</body>
</html>