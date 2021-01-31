<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<cpn:component var="model" type="com.composum.platform.replication.model.ReplicationConfigNode" scope="request">
    <div class="composum-platform-replication-node${model.editable?' editable':''}">
        <sling:call script="form-hidden.jsp"/>
        <sling:call script="form-content.jsp"/>
        <sling:call script="form-extension.jsp"/>
        <div class="form-group">
            <input type="hidden" name="jcr:description@Delete" value="true"/>
            <label class="control-label">${cpn:i18n(slingRequest,'Description')}</label>
            <div class="composum-platform-replication-node_description composum-widgets-richtext richtext-widget widget form-control"
                 data-rules="blank">
                            <textarea name="jcr:description" style="height: 120px;"
                                      class="composum-widgets-richtext_value rich-editor">${model.description}</textarea>
            </div>
        </div>
    </div>
</cpn:component>
