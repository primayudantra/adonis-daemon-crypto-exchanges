  var pm2 = require('pm2');

  pm2.connect(function(err) {
    if (err) throw err;

  setTimeout(function worker() {
      console.log("Restarting app...");
      pm2.restart('data-collector', function() {});
      setTimeout(worker, 1000 * 60 * 60);
    }, 1000);
  });
