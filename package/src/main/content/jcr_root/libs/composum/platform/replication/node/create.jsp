<%@page session="false" pageEncoding="UTF-8" %>
<%@taglib prefix="sling" uri="http://sling.apache.org/taglibs/sling/1.2" %>
<%@taglib prefix="cpn" uri="http://sling.composum.com/cpnl/1.0" %>
<sling:defineObjects/>
<div class="composum-platform-replication-node_dialog dialog modal fade" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content form-panel default">
            <cpn:form action="${cpn:filter(slingRequest.requestPathInfo.suffix)}/*" method="POST"
                      class="composum-platform-replication-node_dialog_form widget-form">
                <div class="modal-header">
                    <button type="button" class="modal-dialog_close fa fa-close" data-dismiss="modal"
                            title="${cpn:i18n(slingRequest,'Close')}"
                            aria-label="${cpn:i18n(slingRequest,'Close')}"></button>
                    <h4 class="modal-title">${cpn:i18n(slingRequest,'New Replication Configuration')}</h4>
                </div>
                <div class="composum-platform-replication-node_dialog_body modal-body">
                    <div class="composum-platform-replication-node_dialog_messages messages">
                        <div class="panel panel-warning hidden">
                            <div class="panel-heading"></div>
                            <div class="panel-body hidden"></div>
                        </div>
                    </div>
                    <input type="hidden" name="_charset_" value="UTF-8"/>
                    <input type="hidden" name="editable@TypeHint" value="Boolean"/>
                    <input type="hidden" name="editable" value="true"/>
                    <div class="row">
                        <div class="col col-xs-6">
                            <div class="form-group">
                                <label>${cpn:i18n(slingRequest,'Type')}</label>
                                <select name="replicationType" data-rules="required"
                                        data-options="remote:${cpn:i18n(slingRequest,'Standard Remote')},inplace:${cpn:i18n(slingRequest,'In-Place')}"
                                        class="composum-platform-replication-node_type widget select-widget form-control"></select>
                            </div>
                        </div>
                        <div class="col col-xs-6">
                            <div class="form-group">
                                <label>${cpn:i18n(slingRequest,'Name')}</label>
                                <input type="text" name=":name" data-rules="required"
                                       data-pattern="^[a-zA-Z][a-zA-Z0-9_-]*$"
                                       class="composum-platform-replication-node_name widget text-field-widget form-control"/>
                            </div>
                        </div>
                    </div>
                    <div class="composum-platform-replication-node_dialog_content">
                    </div>
                </div>
                <div class="composum-platform-replication-node_dialog_footer modal-footer buttons">
                    <button type="button" class="btn btn-default"
                            data-dismiss="modal">${cpn:i18n(slingRequest,'Cancel')}</button>
                    <button type="submit" class="btn btn-primary">${cpn:i18n(slingRequest,'Create')}</button>
                </div>
            </cpn:form>
        </div>
    </div>
</div>
