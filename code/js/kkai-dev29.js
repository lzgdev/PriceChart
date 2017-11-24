var  wss_socket = null;
var  chan_data_OBJs = [];

import { ClChanData_ACandles_HighCharts, ClChanData_ABooks_HighCharts, } from './kkai-dev22.js';

function cbEV_OnDocReady_highcharts()
{
  var mi;
  var mapWREQs = [
//*
        { channel:    'book', uid: 'dep-book-P0', prec: 'P0', len: 100, visible:  true, },
        { channel:    'book', uid: 'dep-book-P1', prec: 'P1', len: 100, visible:  true, },
// */
/*
        { channel:    'book', uid: 'dep-book-P0', prec: 'P0', len: 100, visible: false, },
        { channel:    'book', uid: 'dep-book-P1', prec: 'P1', len: 100, visible: false, },
        { channel: 'candles', uid: 'dep-book-P0', key: 'trade:1m:tBTCUSD', visible:  true, },
// */
      ];

  for (mi=0; mi < mapWREQs.length; mi++)
  {
    var chan_obj;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'book') {
      var chart_gui;
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
            step: true,
            color: '#009F00',
            data: [ ],
          },
          {
            name: 'Tick',
            step: true,
            color: '#FFD700',
            data: [ ],
          },
          {
            name: 'Asks',
            step: true,
            color: '#9F0000',
            data: [ ],
          },
        ],
      });
      chan_obj = new ClChanData_ABooks_HighCharts(chart_gui, map_unit.prec, map_unit.len);
    }
    else
    if (map_unit.channel == 'candles') {
      var  chart_gui;
/*
      chart_gui = Highcharts.stockChart(map_unit.uid, {
        chart: {
            type: 'candlestick',
zoomType: 'x',
            backgroundColor: '#1F1F1F',
        },
        title: {
            text: 'History: ' + map_unit.key,
        },
        subtitle: {
            text: 'Subtitle ...',
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
        series: [
          {
            type: 'candlestick',
            data: [ ],
            dataGrouping: {
                enabled: false
            }
          },
        ],
      });
// */
      chart_gui = null;
      chan_obj = new ClChanData_ACandles_HighCharts(1000, chart_gui, map_unit.key);
    }

    if (chan_obj != null) {
      chan_data_OBJs.push(chan_obj);
    }
  }
}

var flg_dbg_out2 = false;

var num_msg_rcv = 0;
var num_msg_max = 100;

num_msg_max = 0;

function wssBfx_Subscribe(wss_socket)
{
  var obj_chan;
  $('#log_out2').append('\nwssBfx_Subscribe ...');
  for (var i=0; i <  chan_data_OBJs.length; i++)
  {
    var  obj_subscribe = null;
    obj_chan = chan_data_OBJs[i];
    if (obj_chan.name_chan == 'book')
    {
      obj_subscribe = {
        'event': 'subscribe', 'channel': obj_chan.name_chan,
        'symbol': 'tBTCUSD',
        'prec': obj_chan.req_book_prec,
        'freq': 'F0',
        'len':  obj_chan.req_book_len,
      };
    }
    else
    if (obj_chan.name_chan == 'candles')
    {
      obj_subscribe = {
        'event': 'subscribe', 'channel': obj_chan.name_chan,
        'key': obj_chan.req_candles_key,
      };
    }
    if (obj_subscribe != null) {
      wss_socket.send(JSON.stringify(obj_subscribe));
$('#log_out2').append('\nwssBfx_Subscribe: ' + JSON.stringify(obj_subscribe));
    }
  }
}

function wssBfx_OnMsg(obj_msg)
{
  var obj_chan;
  var cid_msg;
  if (obj_msg === null) {
    return;
  }
  if (Array.isArray(obj_msg))
  {
    var handler_msg = null;
    cid_msg = Number(obj_msg[0]);
    for (var i=0; i <  chan_data_OBJs.length; i++) {
      obj_chan = chan_data_OBJs[i];
      if (cid_msg === obj_chan.chan_id) {
        handler_msg = obj_chan;
        break;
      }
    }
    if (handler_msg == null) {
      $('#log_out2').append('\nwssBfx_OnMsg(unk): chanid:' + cid_msg);
    }
    else {
//      $('#log_out2').append('\nwssBfx_OnMsg(obj): chanid:' + cid_msg);
      handler_msg.locAppendData(obj_msg);
    }
  }
  else
  if (obj_msg.event === 'subscribed')
  {
    var handler_msg = null;
    cid_msg = Number(obj_msg.chanId);
    for (var i=0; i <  chan_data_OBJs.length; i++) {
      obj_chan = chan_data_OBJs[i];
      if (obj_chan.name_chan != obj_msg.channel) {
        continue;
      }
      if ((obj_msg.channel == 'book') &&
          (obj_msg.prec == obj_chan.req_book_prec) &&
          (obj_msg.len  == chan_data_OBJs[i].req_book_len)) {
        handler_msg = obj_chan;
        break;
      }
      else
      if ((obj_msg.channel == 'candles') &&
          (obj_msg.key == obj_chan.req_candles_key)) {
        handler_msg = obj_chan;
        break;
      }
    }
    if (handler_msg == null) {
      $('#log_out2').append('\nwssBfx_OnMsg: event:' + obj_msg.event + ', chanId:' + cid_msg + ', obj:' + JSON.stringify(obj_msg));
    }
    else {
      handler_msg.locSet_ChanId(cid_msg);
      if (flg_dbg_out2) $('#log_out2').append('\nwssBfx_OnMsg: event(book):' + obj_msg.event);
    }
  }
  else
  {
    // typeof(obj_msg.event) !== 'undefined'
    $('#log_out2').append('\nwssBfx_OnMsg: obj:' + JSON.stringify(obj_msg));
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
  };
  // Log errors
  wss_socket.onerror = function (error) {
    $('#log_out2').html('WebSocket Error: ' + error);
  };
  // Log messages from the server
  wss_socket.onmessage = function (msg)
  {
    var  obj_msg;
    ++ num_msg_rcv;
    if (flg_dbg_out2) $('#log_out2').append('\nServer(' + num_msg_rcv + '): ' + msg.data);
    obj_msg  = (typeof(msg.data) === 'string') ? JSON.parse(msg.data) : null;
    //console.log('Server: ' + msg.data);
    if ((obj_msg != null) && (obj_msg.event == 'info')) {
      $('#log_out2').append('\nMessage: info=' + JSON.stringify(obj_msg));
      wssBfx_Subscribe(wss_socket);
    }
    else
    if (obj_msg != null) {
      wssBfx_OnMsg(obj_msg);
    }
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

