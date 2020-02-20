<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <div class="row">
        <div class="col col-xs-3">
            <div class="form-group">
                <input type="hidden" name="proxy@Delete" value="false" class="sling-post-delete-hint"/>
                <label>${cpn:i18n(slingRequest,'Proxy')}</label>
                <select name="proxyKey" data-value="${model.property.proxyKey}"
                        data-options=":${cpn:i18n(slingRequest,'no proxy')},proxy1,proxy2"
                        class="composum-platform-replication-config-node_proxy widget select-widget form-control"></select>
            </div>
        </div>
        <div class="col col-xs-9">
            <div class="form-group">
                <label>${cpn:i18n(slingRequest,'Target URL')}</label>
                <input type="text" name="targetUrl" value="${model.property.targetUrl}" data-rules="required"
                       class="composum-platform-replication-config-node_target-url form-control widget text-field-widget"/>
            </div>
            <div class="form-group">
                <label>${cpn:i18n(slingRequest,'Credentials')}</label>
                <input type="text" name="credentialsKey" value="${model.property.credentialsKey}"
                       class="composum-platform-replication-config-node_credentials form-control widget text-field-widget"/>
            </div>
        </div>
    </div>
</cpn:component>
