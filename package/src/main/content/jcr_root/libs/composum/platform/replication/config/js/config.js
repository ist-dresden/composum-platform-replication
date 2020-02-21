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
                _dialog: '_dialog',
                _content: '_content',
                _toolbar: '_toolbar',
                _add: '_add',
                _view: '_view',
                _form: '_form',
                _type: '_type',
                _title: '_title'
            },
            url: {
                base: '/libs/composum/platform/replication/config',
                setup: '/libs/composum/platform/replication/config/setup.reload.html',
                create: '/libs/composum/platform/replication/config/node.create.html',
                delete: '/libs/composum/platform/replication/config/node.delete.html'
            }
        });

        replication.profile = replication.profile || new core.LocalProfile('composum.platform.replication');

        config.ConfigDialog = components.FormDialog.extend({

            initialize: function (options) {
                this.setupForm = options.setupForm;
                this.data = {
                    path: options.path,
                    type: options.type
                };
                components.FormDialog.prototype.initialize.call(this, options);
                this.$('button.delete').click(_.bind(this.deleteConfig, this));
            },

            deleteConfig: function () {
                core.openFormDialog(config.const.url.delete + this.data.path, components.FormDialog,
                    undefined, undefined, _.bind(function () {
                        this.configNode.setupForm.configSetup.reload();
                    }, this));
            }
        });

        config.CreateDialog = components.FormDialog.extend({

            initialize: function (options) {
                this.configNode = options.configNode;
                this.data = {
                    path: options.path
                };
                var c = config.const.css;
                components.FormDialog.prototype.initialize.call(this, options);
                this.$content = this.$('.' + c.base + c._node + c._dialog + c._content);
                this.type = core.getWidget(this.$el, '.' + c.base + c._node + c._type, components.SelectWidget);
                this.type.changed('CreateConfig', _.bind(this.onTypeSelected, this));
                this.onTypeSelected(undefined, this.type.getValue());
            },

            onTypeSelected: function (event, type) {
                core.getHtml(config.const.url.base + '/' + type + '.empty.html' + this.data.path,
                    _.bind(function (content) {
                        this.$content.html(content);
                        CPM.widgets.setUp(this.$content);
                    }, this));
            }
        });

        config.ConfigNode = Backbone.View.extend({

            initialize: function (options) {
                this.setupForm = options.setupForm;
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
                core.openFormDialog(url, config.ConfigDialog, {
                    configNode: this,
                    path: this.$el.data('path'),
                    type: this.$el.data('type')
                }, undefined, _.bind(this.reload, this));
                return false;
            },

            reload: function () {
                var url = '/libs/' + this.$el.data('type') + '.reload.html' + this.$el.data('path');
                core.getHtml(url, _.bind(function (content) {
                    this.$el.html(content);
                    this.initContent();
                }, this));
            }
        });

        config.SetupForm = components.FormWidget.extend({

            initialize: function (options) {
                this.configSetup = options.configSetup;
                var c = config.const.css;
                components.FormWidget.prototype.initialize.call(this, options);
                this.tabbed.$nav.find('a[data-key="' + replication.profile.get('setup', 'formTab', 'path') + '"]').tab('show');
                this.tabbed.$nav.find('a').on('shown.bs.tab.FormTabs', _.bind(function (event) {
                    var $tab = $(event.target);
                    replication.profile.set('setup', 'formTab', $tab.data('key'));
                }, this));
                var that = this;
                this.$('.' + c.base + c._node + c._view + '.editable').each(function () {
                    core.getView($(this), config.ConfigNode, {setupForm: that});
                });
                this.$('.' + c.base + c._setup + c._add).click(_.bind(this.addConfig, this));
            },

            addConfig: function (event) {
                event.preventDefault();
                core.openFormDialog(config.const.url.create + this.configSetup.$el.data('path'),
                    config.CreateDialog, {
                        setupForm: this,
                        path: this.configSetup.$el.data('path')
                    }, undefined, _.bind(function () {
                        this.configSetup.reload();
                    }, this));
                return false;
            }
        });

        config.ConfigSetup = Backbone.View.extend({

            initialize: function (options) {
                this.initContent();
            },

            initContent: function () {
                var c = config.const.css;
                this.form = core.getWidget(this.$el, '.' + c.base + c._setup + c._form, config.SetupForm, {configSetup: this});
            },

            reload: function () {
                core.getHtml(config.const.url.setup + this.$el.data('path'), _.bind(function (content) {
                    this.$el.html(content);
                    this.initContent();
                }, this));
            }
        });

    })(CPM.platform.replication.config, CPM.platform.replication, CPM.core.components, CPM.core);

})();
