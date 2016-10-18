var EmberApp = require('ember-cli/lib/broccoli/ember-app');

module.exports = function (defaults) {
  var app = new EmberApp(defaults, {
    fingerprint: {
      enabled: false
    },
    outputPaths: {
      app: {
        css: {
          'app': '/assets/dr-elephant.css'
        },
        js: '/assets/dr-elephant.js',

      },
      vendor: {
        css: '/assets/vendor.css',
        js: '/assets/vendor.js'
      }
    }
    // Add options here
  });
  app.import(app.bowerDirectory + '/bootstrap/dist/css/bootstrap.css');
  app.import(app.bowerDirectory + '/bootstrap/dist/js/bootstrap.js');

  app.import(app.bowerDirectory + '/bootstrap/dist/fonts/glyphicons-halflings-regular.woff', {
    destDir: 'fonts'
  });
  return app.toTree();
};
