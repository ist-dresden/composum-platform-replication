<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationSetup">
    <form class="composum-platform-replication-setup_form widget-form">
        <ul class="composum-commons-form-tab-nav nav nav-tabs" role="tablist">
        </ul>
        <div class="composum-platform-replication-setup_panels tab-content composum-commons-form-tabbed">
            <div id="${model.domId}_path" data-key="path"
                 data-label="${cpn:i18n(slingRequest,'by Path')}"
                 class="composum-commons-form-tab-panel tab-pane" role="tabpanel">
                <c:forEach items="${model.setupByPath}" var="group">
                    <div class="panel panel-default">
                        <div id="${model.domId}_path-head_${group.key}" role="tab"
                             class="panel-heading">
                            <h4 class="panel-title">
                                <a href="#${model.domId}_path-body_${group.key}"
                                   role="button" data-toggle="collapse" aria-expanded="true"
                                   aria-controls="${model.domId}_path-body_${group.key}">${cpn:text(group.key)}</a>
                            </h4>
                        </div>
                        <div id="${model.domId}_path-body_${group.key}" role="tabpanel"
                             aria-labelledby="${model.domId}_path-head_${group.key}"
                             class="panel-collapse collapse in">
                            <div class="panel-body">
                                <c:forEach items="${group.value}" var="item">
                                    <sling:include path="${item.path}"
                                                   resourceType="${item.replicationType.configResourceType}"/>
                                </c:forEach>
                            </div>
                        </div>
                    </div>
                </c:forEach>
            </div>
            <div id="${model.domId}_type" data-key="type"
                 data-label="${cpn:i18n(slingRequest,'by Type')}"
                 class="composum-commons-form-tab-panel tab-pane" role="tabpanel">
                <c:forEach items="${model.setupByType}" var="item">

                </c:forEach>
            </div>
        </div>
    </form>
</cpn:component>
