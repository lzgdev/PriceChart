
// https://www.npmjs.com/package/ntp-client

var ntpClient = require('ntp-client');
var sleep = require('sleep');
 
ntpClient.getNetworkTime("pool.ntp.org", 123, function(err, date) {
    if(err) {
        console.error(err);
        return;
    }
    console.log("NTP:", date, "Time:", date.getTime());

    var now, diff;
    var d = new Date();
    var n = d.getTimezoneOffset();

    diff = date.getTime() - d.getTime();
    console.log("Date:", d, "TZ offset:", n, "Diff:", diff);

    now  = Date.now();
    diff = now - date.getTime();
    console.log("Now:", now, "Diff:", diff);
    sleep.sleep(3);
    now  = Date.now();
    diff = now - date.getTime();
    console.log("Now:", now, "Diff:", diff);
});

