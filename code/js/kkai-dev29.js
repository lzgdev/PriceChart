var  wss_socket = null;
var  chan_book_OBJs = [];

import { ClChanData_ABooks_HighCharts, } from './kkai-dev22.js';

function cbEV_OnDocReady_highcharts()
{
  var map_wreq2uid = [
//        { prec: 'P0', len:  25, uid: 'dep-book-P0', visible:  true, },
//        { prec: 'P1', len:  25, uid: 'dep-book-P1', visible: false, },
        { prec: 'P0', len: 100, uid: 'dep-book-P0', visible:  true, },
        { prec: 'P1', len: 100, uid: 'dep-book-P1', visible:  true, },
//        { prec: 'P2', len: 100, uid: 'dep-book-P2', visible:  true, },
//        { prec: 'P3', len: 100, uid: 'dep-book-P3', visible: false, },
      ];

  for (var m=0; m < map_wreq2uid.length; m++)
  {
    var books_obj, chart_gui;
    var chart;
    var series, seriesData;
    var map_unit = map_wreq2uid[m];
    if (!map_unit.visible) {
      continue;
    }
    chart_gui = Highcharts.chart(map_unit.uid, {
        chart: {
            type: 'area',
            backgroundColor: '#1F1F1F',
        },
        title: {
            text: 'Books Depth: ' + map_unit.prec,
        },
        yAxis: {
            min: 0.0,
            title: {
                text: 'Amount Sum',
            },
            stackLabels: {
                enabled: true,
                style: {
                    fontWeight: 'bold',
                    color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
                }
            }
        },
        tooltip: {
            headerFormat: '<b>{point.x}</b><br/>',
            pointFormat: '{series.name}: {point.y}<br/>Total: {point.stackTotal}'
        },
        plotOptions: {
            column: {
                stacking: 'normal',
                dataLabels: {
                    enabled: true,
                    color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                }
            }
        },
    
        series: [
          {
            name: 'Bids',
            color: '#009F00',
            data: [ ],
          },
          {
            name: 'Tick',
            color: '#FFD700',
            data: [ ],
          },
          {
            name: 'Asks',
            color: '#9F0000',
            data: [ ],
          },
        ],
      });

    books_obj = new ClChanData_ABooks_HighCharts(map_unit.prec, map_unit.len, chart_gui);
    chan_book_OBJs.push(books_obj);
  }

}

var flg_dbg_out2 = false;

var num_msg_rcv = 0;
var num_msg_max = 100;

num_msg_max = 0;

function wssBfx_OnMsg(msg)
{
  var obj_chan;
  var obj_msg  = (typeof(msg.data) === 'string') ? JSON.parse(msg.data) : null;
  var cid_msg;
  if (obj_msg === null) {
    return;
  }
//  $('#log_out2').append('\nServer: msg=' + msg.data);
  if (Array.isArray(obj_msg))
  {
    var handler_msg = null;
    cid_msg = Number(obj_msg[0]);
    for (var i=0; i <  chan_book_OBJs.length; i++) {
      obj_chan = chan_book_OBJs[i];
      if (cid_msg === obj_chan.chan_id) {
        handler_msg = obj_chan;
        break;
      }
    }
    if (handler_msg !== null) {
      handler_msg.locAppendData(obj_msg);
      //$('#log_out2').append('\nwssBfx_OnMsg: chanid(books):' + cid_msg);
/*
      $('#log_out2').append('\nwssBfx_OnMsg: bids=' + JSON.stringify(handler_msg.loc_book_bids));
      $('#log_out2').append('\nwssBfx_OnMsg: asks=' + JSON.stringify(handler_msg.loc_book_asks));
// */
    }
    else {
      $('#log_out2').append('\nwssBfx_OnMsg: chanid:' + cid_msg);
    }
  }
  else
  if (obj_msg.event === 'subscribed')
  {
    cid_msg = Number(obj_msg.chanId);
    if (obj_msg.channel === 'book' && obj_msg.symbol === 'tBTCUSD')
    {
      var handler_msg = null;
      for (var i=0; i <  chan_book_OBJs.length; i++) {
        obj_chan = chan_book_OBJs[i];
        if ((obj_msg.prec == obj_chan.req_book_prec) &&
            (obj_msg.len  == chan_book_OBJs[i].req_book_len)) {
          handler_msg = obj_chan;
          break;
        }
      }
      if (handler_msg != null) {
        handler_msg.locSet_ChanId(cid_msg);
      }
      if (flg_dbg_out2) $('#log_out2').append('\nwssBfx_OnMsg: event(book):' + obj_msg.event);
    }
    else {
      $('#log_out2').append('\nwssBfx_OnMsg: event:' + obj_msg.event + ', chanId:' + cid_msg + ', msg:' + msg.data);
    }
  }
  else
  {
    // typeof(obj_msg.event) !== 'undefined'
    $('#log_out2').append('\nwssBfx_OnMsg: msg:' + msg.data);
  }
}

function cbEV_OnDocReady_websocket()
{
  var wss_srvproto = 'wss';
  var wss_srvhost = 'api.bitfinex.com';
  var wss_srvport = null;
  var wss_srvpath = null;

  wss_srvpath = '/ws/2';

  wss_socket = new WebSocket(wss_srvproto + '://' + wss_srvhost
                    + ((wss_srvport === null) ? '' : (':' + wss_srvport))
                    + ((wss_srvpath === null) ? '' : wss_srvpath));

  // When the connection is open, send some data to the server
  wss_socket.onopen = function ()
  {
    //wss_socket.send('Ping'); // Send the message 'Ping' to the server
    $('#log_out2').html('wss_socket connected to ' + wss_srvhost);
    for (var i=0; i <  chan_book_OBJs.length; i++)
    {
      var obj_subscribe = {
        'event': 'subscribe', 'channel': 'book', 'symbol': 'tBTCUSD',
        'prec': chan_book_OBJs[i].req_book_prec,
        'freq': 'F0',
        'len':  chan_book_OBJs[i].req_book_len,
      };
      wss_socket.send(JSON.stringify(obj_subscribe));
    }
  };
  // Log errors
  wss_socket.onerror = function (error) {
    $('#log_out2').html('WebSocket Error: ' + error);
  };

  // Log messages from the server
  wss_socket.onmessage = function (msg) {
    ++ num_msg_rcv;
    if (flg_dbg_out2) $('#log_out2').append('\nServer(' + num_msg_rcv + '): ' + msg.data);
    //console.log('Server: ' + msg.data);
    wssBfx_OnMsg(msg);
    if ((num_msg_max != 0) && (num_msg_rcv >= num_msg_max)) {
      $('#log_out2').append('\nClose websocket!!');
      wss_socket.onclose = function () {}; // disable onclose handler first
      wss_socket.close();
    }
  };

  wss_socket.onerror = function (error) {
  };
}

function _onUI_Test01()
{
  $('#log_out1').html('Close websocket.');
  num_msg_max = -1;
}

function _onUI_Test02()
{
  $('#log_out1').html('Test 02.');
}

export { cbEV_OnDocReady_highcharts, cbEV_OnDocReady_websocket, };
export { _onUI_Test01, _onUI_Test02, };

