<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<div class="composum-platform-replication-node_dialog dialog modal fade" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content form-panel default">
            <cpn:form action="${slingRequest.requestPathInfo.suffix}" method="POST"
                      class="composum-platform-replication-node_dialog_form widget-form">
                <div class="modal-header">
                    <button type="button" class="modal-dialog_close fa fa-close" data-dismiss="modal"
                            title="${cpn:i18n(slingRequest,'Close')}"
                            aria-label="${cpn:i18n(slingRequest,'Close')}"></button>
                    <sling:call script="dialog-title.jsp"/>
                </div>
                <div class="composum-platform-replication-node_dialog_body modal-body">
                    <div class="composum-platform-replication-node_dialog_messages messages">
                        <div class="panel panel-warning hidden">
                            <div class="panel-heading"></div>
                            <div class="panel-body hidden"></div>
                        </div>
                    </div>
                    <input name="_charset_" type="hidden" value="UTF-8"/>
                    <div class="composum-platform-replication-node_dialog_content">
                        <sling:include path="${slingRequest.requestPathInfo.suffix}" replaceSelectors="form"/>
                    </div>
                </div>
                <div class="composum-platform-replication-node_dialog_footer modal-footer buttons">
                    <button type="button" class="delete btn btn-danger"
                            data-dismiss="modal">${cpn:i18n(slingRequest,'Delete')}</button>
                    <button type="button" class="btn btn-default"
                            data-dismiss="modal">${cpn:i18n(slingRequest,'Cancel')}</button>
                    <button type="submit" class="btn btn-primary">${cpn:i18n(slingRequest,'Save')}</button>
                </div>
            </cpn:form>
        </div>
    </div>
</div>
