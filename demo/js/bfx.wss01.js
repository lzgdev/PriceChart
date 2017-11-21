
const url_bfx_wss = 'wss://api.bitfinex.com/ws/2';

const WebSocket = require('ws');

const wss = new WebSocket(url_bfx_wss);

var obj_send01 = null;

obj_send01 = { event: 'ping', };
obj_send01 = { event: 'subscribe', channel: 'ticker', symbol: 'tBTCUSD', };
//obj_send01 = { event: 'subscribe', channel: 'trades', symbol: 'tBTCUSD', };
//obj_send01 = { event: 'subscribe', channel: 'ticker', symbol: 'tBTCUSD', };

wss.on('open', function open() {
  if (obj_send01 != null) {
    wss.send(JSON.stringify(obj_send01));
  }
});

wss.on('message', function incoming(data) {
  console.log(data);
});

