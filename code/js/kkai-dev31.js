
const WebSocket = require('ws');

class ClNetClient_Base
{
  constructor()
  {
  }

  addObj_DataReceiver(obj_receiver)
  {
    this.onNetClient_AddReceiver(obj_receiver);
  }

  netClient_exec()
  {
    this.onNetClient_Exec_impl();
  }

  dataPrep_subscribe()
  {
    this.onPrep_Subscribe_impl();
  }

  dataRecv_message(obj_msg)
  {
    this.onData_Message_impl(obj_msg);
  }

  onNetClient_AddReceiver(obj_receiver)
  {
  }

  onNetClient_Exec_impl()
  {
  }

  onPrep_Subscribe_impl()
  {
  }

  onData_Message_impl(obj_msg)
  {
  }
}

class ClNetClient_BfxWss extends ClNetClient_Base
{
  constructor()
  {
    super();
    this.sock_wss  = null;
    this.objs_chan_data = [];
  }

  onNetClient_AddReceiver(obj_receiver)
  {
    if (obj_receiver != null) {
      this.objs_chan_data.push(obj_receiver);
    }
  }

  onPrep_Subscribe_impl()
  {
  //function wssBfx_Subscribe(this.sock_wss)
    var  obj_chan;
    for (var i=0; i <  this.objs_chan_data.length; i++)
    {
      var  obj_subscribe = null;
      obj_chan = this.objs_chan_data[i];
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
        this.sock_wss.send(JSON.stringify(obj_subscribe));
//        $('#log_out2').append('\nwssBfx_Subscribe: ' + JSON.stringify(obj_subscribe));
      }
    }
  }

  onData_Message_impl(obj_msg)
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
      for (var i=0; i <  this.objs_chan_data.length; i++) {
        obj_chan = this.objs_chan_data[i];
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
      for (var i=0; i <  this.objs_chan_data.length; i++) {
        obj_chan = this.objs_chan_data[i];
        if (obj_chan.name_chan != obj_msg.channel) {
          continue;
        }
        if ((obj_msg.channel == 'book') &&
            (obj_msg.prec == obj_chan.req_book_prec) &&
            (obj_msg.len  == this.objs_chan_data[i].req_book_len)) {
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
//        $('#log_out2').append('\nwssBfx_OnMsg: event(book):' + obj_msg.event);
      }
    }
    else
    {
      // typeof(obj_msg.event) !== 'undefined'
      $('#log_out2').append('\nwssBfx_OnMsg: obj:' + JSON.stringify(obj_msg));
    }
  }

  onNetClient_Exec_impl()
  {
    var wss_srvproto = 'wss';
    var wss_srvhost = 'api.bitfinex.com';
    var wss_srvport = null;
    var wss_srvpath = null;

    wss_srvpath = '/ws/2';
    this.sock_wss = new WebSocket(wss_srvproto + '://' + wss_srvhost
                      + ((wss_srvport === null) ? '' : (':' + wss_srvport))
                      + ((wss_srvpath === null) ? '' : wss_srvpath));

    // When the connection is open, send some data to the server
    this.sock_wss.onclose = () => {
    };
    this.sock_wss.onopen  = () => {
      //this.sock_wss.send('Ping'); // Send the message 'Ping' to the server
      console.log('WebSocket connected to ' + wss_srvhost);
    };
    // Log errors
    this.sock_wss.onerror = (error) => {
      console.log('WebSocket Error: ' + error);
    };
    // Log messages from the server
    this.sock_wss.onmessage = (msg) =>
    {
      var  obj_msg;
      obj_msg  = (typeof(msg.data) === 'string') ? JSON.parse(msg.data) : null;
      if ((obj_msg != null) && (obj_msg.event == 'info')) {
        this.dataPrep_subscribe();
      }
      else
      if (obj_msg != null) {
        this.dataRecv_message(obj_msg);
      }
/*
      if ((num_msg_max != 0) && (num_msg_rcv >= num_msg_max)) {
        $('#log_out2').append('\nClose websocket!!');
        this.sock_wss.close();
      }
// */
    };
    this.sock_wss.onerror = (error) => {
    };
  }
}

//export { ClNetClient_BfxWss, ClNetClient_Base, };

module.exports = {
  ClNetClient_Base:   ClNetClient_Base,
  ClNetClient_BfxWss: ClNetClient_BfxWss,
}

