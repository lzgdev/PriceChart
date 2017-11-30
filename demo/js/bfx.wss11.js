
const dev31 = require('../../code/js/kkai-dev31.js');

const dev51 = require('../../code/js/kkai-dev51.js');
const dev52 = require('../../code/js/kkai-dev52.js');

var obj_netclient = null;
var obj_dbwritter = null;


var mapWREQs = [
        { channel:  'ticker', uid: 'container-ticker', visible:  true, wreq_args: { symbol: 'tBTCUSD', }, },
        { channel:    'book', uid: 'container-bookP0', visible:  true, wreq_args: { symbol: 'tBTCUSD', prec: 'P0', freq: 'F0', len: 100, }, },
        { channel:    'book', uid: 'container-bookP1', visible: false, wreq_args: { symbol: 'tBTCUSD', prec: 'P1', freq: 'F0', len: 100, }, },
        { channel: 'candles', uid: 'container-candle', visible: false, wreq_args: { key: 'trade:1m:tBTCUSD', }, },
      ];

var db_url_p  = "mongodb://localhost:27017";
var db_name   = "bfx-pub";
var coll_name = "info0";

process.on('SIGINT', function() {
  console.log("WARN: caught interrupt signal ...");
/*
  if (obj_dbwritter != null) {
    obj_dbwritter.dbOP_Close();
  }
  else {
    process.exit();
  }
// */
    process.exit();
});

console.log("main(1) ...");

obj_dbwritter = new dev52.ClDataSet_DbWriter();

obj_dbwritter.dbOP_Connect(db_url_p + '/' + db_name, coll_name);

//*
obj_netclient = new dev31.ClNetClient_BfxWss();

for (var mi=0; mi < mapWREQs.length; mi++)
{
    var chan_obj;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'ticker') {
      chan_obj = new dev51.ClDataSet_Ticker_DbOut(obj_dbwritter, map_unit.wreq_args);
    }
    else
    if (map_unit.channel == 'book') {
      chan_obj = new dev51.ClDataSet_ABooks_DbOut(obj_dbwritter, map_unit.wreq_args);
    }
    else
    if (map_unit.channel == 'candles') {
      chan_obj = new dev51.ClDataSet_ACandles_DbOut(1000, obj_dbwritter, map_unit.wreq_args);
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
}

obj_netclient.ncOP_Exec();
// */

