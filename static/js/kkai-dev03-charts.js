

var flag_book_P0 = true;
var flg_dbg_out2 = false;

var num_msg_rcv = 0;
var num_msg_max = 100;

var  wss_socket = null;
var  chan_book_OBJs = [];

var  valMax = 1000.0;

num_msg_max = 0;
valMax = 200.0;

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
        if (obj_msg.prec == obj_chan.prec) {
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

function cbEV_OnDocReady_anychart()
{
  var chart;
  var series, seriesData;
  var str_prec;
  var books_obj = null;

  str_prec = 'P0';
  str_prec = 'P1';
  books_obj = new ClChanData_ABooks_AnyChart(str_prec);
  chan_book_OBJs.push(books_obj);

  // create a chart
  chart = anychart.column();

  chart.background().fill("#2F4F4F");

  // enable the percent stacking mode
  chart.yScale().stackMode("value");

  // create splineArea series, set the data
  seriesData = books_obj.loc_anychart_dataset.mapAs({x: 0, value: 5, fill: 2, });
  series = chart.column(seriesData);
  seriesData = books_obj.loc_anychart_dataset.mapAs({x: 0, value: 4, fill: 1, });
  series = chart.column(seriesData);

  // configure tooltips
  chart.tooltip().format("{%Value} ({%yPercentOfCategory}{decimalCount:2}%)");

  // configure labels on the Y-axis
  chart.yAxis().labels().format("{%Value}");
  chart.yAxis().orientation("left");
//  chart.yAxis().orientation("right");

  chart.yScale().minimum(0);
  chart.yScale().maximum(valMax);

  // set the chart title
  chart.title("Stacked Column Chart");

  // set the container id
  chart.container("dep01-book");

  chart.draw();
}

function cbEV_OnDocReady_websocket()
{
  var num_books = 25;

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
        'prec': chan_book_OBJs[i].prec,
        'freq': 'F0',
        'len':  num_books,
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

