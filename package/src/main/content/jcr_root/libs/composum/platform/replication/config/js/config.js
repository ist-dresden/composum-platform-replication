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
                _setup: '-setup',
                _form: '_form'
            },
            url: {
                dialog: {
                    base: '/libs/composum/platform/replication/config'
                }
            }
        });

        replication.profile = replication.profile || new core.LocalProfile('composum.platform.replication');

        config.SetupForm = components.FormWidget.extend({

            initialize: function (options) {
                var c = config.const.css;
                components.FormWidget.prototype.initialize.call(this, options);
                this.tabbed.$nav.find('a[data-key="' + replication.profile.get('setup', 'formTab', 'path') + '"]').tab('show');
                this.tabbed.$nav.find('a').on('shown.bs.tab.FormTabs', _.bind(function (event) {
                    var $tab = $(event.target);
                    replication.profile.set('setup', 'formTab', $tab.data('key'));
                }, this));
            }
        });

        config.ConfigSetup = Backbone.View.extend({

            initialize: function (options) {
                var c = config.const.css;
                this.form = core.getWidget(this.$el, '.' + c.base + c._setup + c._form, config.SetupForm);
            }
        });

    })(CPM.platform.replication.config, CPM.platform.replication, CPM.core.components, CPM.core);

})();
