/**
 *
 *
 */
(function () {
    'use strict';
    CPM.namespace('platform.replication.config');

    (function (config, replication, components, core) {

        config.const = _.extend(config.const || {}, {
            css: {
                base: 'composum-platform-replication-config',
                _node: '-node',
                _setup: '-setup',
                _view: '_view',
                _form: '_form',
                _title: '_title'
            }
        });

        replication.profile = replication.profile || new core.LocalProfile('composum.platform.replication');

        config.ConfigNode = Backbone.View.extend({

            initialize: function (options) {
                this.initContent();
                this.$el.click(_.bind(this.openDialog, this));
            },

            initContent: function () {
                var c = config.const.css;
                this.$title = this.$('.' + c.base + c._node + c._title);
                this.$title.tooltip({html: true, placement: 'bottom'});
            },

            openDialog: function (event) {
                event.preventDefault();
                var url = '/libs/' + this.$el.data('type') + '.dialog.html' + this.$el.data('path');
                core.openFormDialog(url, components.FormDialog, undefined, undefined,
                    _.bind(function () {
                        var url = '/libs/' + this.$el.data('type') + '.reload.html' + this.$el.data('path');
                        core.getHtml(url, _.bind(function (content) {
                            this.$el.html(content);
                            this.initContent();
                        }, this));
                    }, this));
                return false;
            }
        });

        config.SetupForm = components.FormWidget.extend({

            initialize: function (options) {
                var c = config.const.css;
                components.FormWidget.prototype.initialize.call(this, options);
                this.tabbed.$nav.find('a[data-key="' + replication.profile.get('setup', 'formTab', 'path') + '"]').tab('show');
                this.tabbed.$nav.find('a').on('shown.bs.tab.FormTabs', _.bind(function (event) {
                    var $tab = $(event.target);
                    replication.profile.set('setup', 'formTab', $tab.data('key'));
                }, this));
                this.$('.' + c.base + c._node + c._view + '.editable').each(function () {
                    core.getView($(this), config.ConfigNode);
                });
            }
        });

        config.ConfigSetup = Backbone.View.extend({

            initialize: function (options) {
                var c = config.const.css;
                this.form = core.getWidget(this.$el, '.' + c.base + c._setup + c._form, config.SetupForm);
            },

            refresh: function () {

            }
        });

    })(CPM.platform.replication.config, CPM.platform.replication, CPM.core.components, CPM.core);

})();
