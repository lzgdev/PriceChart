
const dev31 = require('../../code/js/kkai-dev31.js');
const dev51 = require('../../code/js/kkai-dev51.js');

var obj_netclient = null;

var mapWREQs = [
/*
        { channel:    'book', uid: 'container-bookP0', prec: 'P0', len: 100, visible: true, },
        { channel:    'book', uid: 'container-bookP1', prec: 'P1', len: 100, visible: true, },
        { channel: 'candles', uid: 'container-candle', key: 'trade:1m:tBTCUSD', visible: true, },
// */
        { channel:    'book', uid: 'container-bookP0', prec: 'P0', len: 100, visible:  true, },
        { channel:    'book', uid: 'container-bookP1', prec: 'P1', len: 100, visible: false, },
        { channel: 'candles', uid: 'container-candle', key: 'trade:1m:tBTCUSD', visible: false, },
      ];

obj_netclient = new dev31.ClNetClient_BfxWss();

for (var mi=0; mi < mapWREQs.length; mi++)
{
    var chan_obj, chart_gui;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'book') {
      chart_gui = null;
      chan_obj = new dev51.ClDataSet_ABooks_DbOut(chart_gui, map_unit.prec, map_unit.len);
    }
    else
    if (map_unit.channel == 'candles') {
      chart_gui = null;
      chan_obj = new dev51.ClDataSet_ACandles_DbOut(1000, chart_gui, map_unit.key);
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
}

obj_netclient.netClient_exec();


/*
var dev52 = require('../../code/js/kkai-dev52.js');

var db_url = "mongodb://localhost:27017/mydb";
var col_name = "customers";

var dbObj = null;

console.log("main(1) ...");

dbObj = new dev52.ClDataSet_DbWriter();

dbObj.dbConnect(db_url, col_name);

console.log("main(99), dbObj:", dbObj, "db:", dbObj.db_database, "col:", dbObj.db_collection);
// */

