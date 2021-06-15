'use strict'

const Env = use('Env');
const { sendNotification } = require('../../Services/Telegram')
const WebSocket = require('ws');
const pako = require('pako');
const wsUrl = 'wss://api.hbdm.com/linear-swap-ws';
const files = require('../../../Utils/files');

const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price','ask_price']
const exchangePlatform = 'HUOBI'
let cached_keys = [];

class HuobiWSController {
  constructor(webSocket, cb) {
    this.webSocket = webSocket;
    this.cb = cb;
    this.isConnected = false;
    webSocket.onopen = this.onopen.bind(this);
    webSocket.onclose = this.onclose.bind(this);
    webSocket.onmessage = this.onmessage.bind(this);
    webSocket.onerror = this.onerror.bind(this);
  }

  static listen(cb) {
    let ws;
    try {
      if(Env.get('TYPE') === 'DAEMON'){
        ws = new WebSocket(wsUrl);
        return new HuobiWSController(ws, cb);
      }
    } catch (err) {
      console.log(err.message);
      return null;
    }
  }

  onopen() {
    this.isConnected = true;
    this.startStreaming('market.BTC-USDT.bbo');
    this.startStreaming('market.ETH-USDT.bbo');
  }

  onclose() {

    let e = 'echo-protocol Connection Closed';  
    this.isConnected = false;
    sendNotification(exchangePlatform, e)
  }

  onmessage(message) {
    try {
      let text = pako.inflate(message.data, {
        to: 'string'
      });
      let data = JSON.parse(text);
      if (data.ping) {
        this.startPong(data.ping)
      } else if (data.tick) {
        computeQuote(data)
      }
    } catch (e) {
      console.log(e);
      sendNotification(exchangePlatform, e)
    }
  }

  onerror(err) {
    console.log(err)
    this.cb(`err: ${err.message}`);
    sendNotification(exchangePlatform, e)
  }

  startStreaming(sub) {
    const wsQuery = JSON.stringify({
      id: "id8",
      sub: sub,
    });
    this.webSocket.send(wsQuery);
  }

  startPong(ts) {
    console.log(ts)
    const data = JSON.stringify({ pong : ts});
    this.webSocket.send(data);
  }

}


async function computeQuote(data){
  if (!data) {
    return;
  }
  const pair = data['ch'].split('.')[1];
  const timestamp = data['ts'];

  const unique_key = timestamp + pair;

  const quoteObj = {
    timestamp,
    pair,
    bid_price: data['tick']['bid'][0],
    ask_price: data['tick']['ask'][0],
    platform: exchangePlatform,
    table: 'quote',
  }

  console.log(`${exchangePlatform} : ${JSON.stringify(quoteObj)}`)
  try {
    if (!cached_keys.includes(unique_key)) {
      cached_keys = [...cached_keys, unique_key];

      while (cached_keys.length > 100) {
        cached_keys = cached_keys.slice(1, cached_keys.length);
      }

      files.drive(quoteObj, QUOTE_CSV_HEADER);
    }

  } catch(e) {
    // e means the file doesn't exist
    files.drive(quoteObj, QUOTE_CSV_HEADER)
  }
    
}
module.exports = HuobiWSController
