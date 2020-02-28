<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationSetup" scope="request">
    <div class="composum-platform-replication-setup_panel-path panel-group"
         role="tablist" aria-multiselectable="true">
        <c:forEach items="${model.setupByStage}" var="group">
            <div class="panel panel-default">
                <div id="${model.domId}_path-head_${group.id}" role="tab"
                     class="panel-heading">
                    <h4 class="panel-title">
                        <a href="#${model.domId}_path-body_${group.id}"
                           role="button" data-toggle="collapse" aria-expanded="true"
                           aria-controls="${model.domId}_path-body_${group.id}">${cpn:text(group.title)}</a>
                    </h4>
                </div>
                <div id="${model.domId}_path-body_${group.id}" role="tabpanel"
                     aria-labelledby="${model.domId}_path-head_${group.id}"
                     class="panel-collapse collapse in">
                    <ul class="list-group">
                        <c:forEach items="${group.set}" var="item">
                            <li class="list-group_item">
                                <sling:include path="${item.path}"
                                               resourceType="${item.configResourceType}" replaceSelectors=""/>
                            </li>
                        </c:forEach>
                    </ul>
                </div>
            </div>
        </c:forEach>
    </div>
</cpn:component>
