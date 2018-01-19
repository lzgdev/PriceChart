
class ClNetClient_Base
{
  constructor()
  {
  }

  ncChk_HaveRecvs()
  {
    return this.onNcChk_HaveRecvs_impl();
  }

  addObj_DataReceiver(obj_receiver)
  {
    this.onNcOP_AddReceiver(obj_receiver);
  }

  ncOP_Exec()
  {
    this.onNcOP_Exec_impl();
  }

  ncOP_Close()
  {
    this.onNcOP_Close_impl();
  }

  ncOP_PrepSubscribe()
  {
    this.onNcOP_PrepSubscribe_impl();
  }

  ncEV_RecvMessage(obj_msg)
  {
    this.onNcEV_Message_impl(obj_msg);
  }

  onNcChk_HaveRecvs_impl()
  {
    return false;
  }

  onNcOP_AddReceiver(obj_receiver)
  {
  }

  onNcOP_Exec_impl()
  {
  }

  onNcOP_Close_impl()
  {
  }

  onNcOP_PrepSubscribe_impl()
  {
  }

  onNcEV_Message_impl(obj_msg)
  {
  }
}

class ClNetClient_BfxWss extends ClNetClient_Base
{
  constructor(url_wss)
  {
    super();
    this.url_wss   = url_wss;
    this.sock_wss  = null;
    this.objs_chan_data = [];
  }

  onNcChk_HaveRecvs_impl()
  {
    return (this.objs_chan_data.length > 0) ? true : false;
  }

  onNcOP_AddReceiver(obj_receiver)
  {
    if (obj_receiver != null) {
      this.objs_chan_data.push(obj_receiver);
    }
  }

  onNcOP_PrepSubscribe_impl()
  {
    var  obj_chan;
    for (var i=0; i <  this.objs_chan_data.length; i++)
    {
      var  obj_subscribe;
      obj_chan = this.objs_chan_data[i];
      obj_subscribe = {
          'event': 'subscribe', 'channel': obj_chan.name_chan,
        };
      for (const prop in obj_chan.wreq_args) {
        obj_subscribe[prop] = obj_chan.wreq_args[prop];
      }
      this.sock_wss.send(JSON.stringify(obj_subscribe));
    }
  }

  onNcEV_Message_impl(obj_msg)
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
        if (cid_msg === obj_chan.id_chan) {
          handler_msg = obj_chan;
          break;
        }
      }
      if (handler_msg == null) {
        console.log("ClNetClient_BfxWss(onNcEV_Message_impl): can't handle data, chanId:", cid_msg, ", obj:", JSON.stringify(obj_msg));
      }
      else {
        //console.log("ClNetClient_BfxWss(onNcEV_Message_impl): handle data, chanId:", cid_msg, ", obj:", JSON.stringify(obj_msg));
        handler_msg.locDataAppend(DFMT_BITFINEX, obj_msg);
      }
    }
    else
    if (obj_msg.event === 'subscribed')
    {
      var handler_msg = null;
      cid_msg = Number(obj_msg.chanId);
      for (var i=0; (handler_msg == null) && (i <  this.objs_chan_data.length); i++)
      {
        obj_chan = this.objs_chan_data[i];
        if (obj_chan.name_chan != obj_msg.channel) {
          continue;
        }
        handler_msg = obj_chan;
        for (const prop in obj_chan.wreq_args) {
          if (obj_chan.wreq_args[prop] != obj_msg[prop]) {
            handler_msg = null;
            break;
          }
        }
      }
      if (handler_msg == null) {
        console.log("ClNetClient_BfxWss(onNcEV_Message_impl): can't handle subscribe, chanId:", cid_msg, ", obj:", JSON.stringify(obj_msg));
      }
      else {
        //console.log("ClNetClient_BfxWss(onNcEV_Message_impl): event(book):", obj_msg.event);
        handler_msg.locSet_ChanId(cid_msg);
      }
    }
    else
    {
      // typeof(obj_msg.event) !== 'undefined'
      console.log("ClNetClient_BfxWss(onNcEV_Message_impl): can't handle obj", JSON.stringify(obj_msg));
    }
  }

  onNcOP_Exec_impl()
  {
    var url_wss_def = 'wss://api.bitfinex.com/ws/2';
    var url_wss;

    url_wss = (this.url_wss == null) ? url_wss_def : this.url_wss;
    this.sock_wss = new WebSocket(url_wss);

    // When the connection is open, send some data to the server
    this.sock_wss.onclose = () => {
    };
    this.sock_wss.onopen  = () => {
      console.log('WebSocket connected to ' + url_wss);
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
        this.ncOP_PrepSubscribe();
      }
      else
      if (obj_msg != null) {
        this.ncEV_RecvMessage(obj_msg);
      }
    };
    this.sock_wss.onerror = (error) => {
    };
  }

  onNcOP_Close_impl()
  {
  }
}

