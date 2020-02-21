<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <div class="row">
        <div class="col col-xs-3">
            <div class="form-group">
                <input type="hidden" name="enabled@TypeHint" value="Boolean" class="sling-post-type-hint"/>
                <input type="hidden" name="enabled@DefaultValue" value="false" class="sling-post-default-hint"/>
                <input type="hidden" name="enabled@UseDefaultWhenMissing" value="true"
                       class="sling-post-use-default-hint"/>
                <label>${cpn:i18n(slingRequest,'active')}</label>
                <input type="checkbox" name="enabled" data-value="${model.enabled}"
                       class="composum-platform-replication-config-node_enabled widget checkbox-widget form-control"/>
            </div>
        </div>
        <div class="col col-xs-9">
            <div class="form-group">
                <input type="hidden" name="jcr:title@Delete" value="true" class="sling-post-delete-hint"/>
                <label>${cpn:i18n(slingRequest,'Title')}</label>
                <input type="text" name="jcr:title" value="${model.property._jcr_title}"
                       class="composum-platform-replication-config-node_title widget text-field-widget form-control"/>
            </div>
        </div>
    </div>
    <div class="row">
        <div class="col col-xs-3">
            <div class="form-group">
                <label>${cpn:i18n(slingRequest,'Stage')}</label>
                <select name="stage" data-value="${model.stage}" data-default="public" data-rules="required"
                        data-options="public:${cpn:i18n(slingRequest,'Public')},preview:${cpn:i18n(slingRequest,'Preview')}"
                        class="composum-platform-replication-config-node_stage widget select-widget form-control"></select>
            </div>
        </div>
        <div class="col col-xs-9">
            <div class="form-group">
                <label>${cpn:i18n(slingRequest,'Source Path')}</label>
                <div data-rules="required"
                     class="composum-platform-replication-config-node_source-path input-group widget path-widget">
                    <input name="sourcePath" class="form-control" type="text" value="${model.property.sourcePath}"/>
                    <span class="input-group-btn"><button
                            class="select btn btn-default" type="button"
                            title="${cpn:i18n(slingRequest,'Select Repository Path')}">...</button></span>
                </div>
            </div>
            <div class="form-group">
                <label>${cpn:i18n(slingRequest,'Target Path')}</label>
                <input type="hidden" name="targetPath@Delete" value="true" class="sling-post-delete-hint"/>
                <input type="text" name="targetPath" value="${model.targetPath}"
                       class="composum-platform-replication-config-node_target-path widget text-field-widget form-control"/>
            </div>
        </div>
    </div>
</cpn:component>
