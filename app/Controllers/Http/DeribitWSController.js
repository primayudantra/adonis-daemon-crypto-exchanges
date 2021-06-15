'use strict'

const Env = use('Env');
const WebSocket = require('ws');
const wsUrl = 'wss://www.deribit.com/ws/api/v2';
const files = require('../../../Utils/files');
const { sendNotification } = require('../../Services/Telegram')
const wsQuery = JSON.stringify({ "method": "public/subscribe", "params": { "channels": [ "quote.BTC-PERPETUAL", "quote.ETH-PERPETUAL" ] }, "jsonrpc": "2.0", "id": 7 })

const wsPing =  JSON.stringify({ "jsonrpc" : "2.0", "id" : 9098, "method" : "public/set_heartbeat", "params" : { "interval" : 10 } })

const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price','ask_price']
const exchangePlatform = 'DERIBIT'

class DeribitWSController {
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
        return new DeribitWSController(ws, cb);
      }
    } catch (err) {
      console.log(err.message);
      return null;
    }
  }

  onopen() {
    this.isConnected = true;
    this.startStreaming();
    this.startPing();
  }

  onclose() {

    let e = 'echo-protocol Connection Closed';  
    this.isConnected = false;
    this.stopPing();
    sendNotification(exchangePlatform, e)
  }

  onmessage(message) {
    try {
      if (message) {
        computeQuote(message.data)
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

  startStreaming() {
    this.webSocket.send(wsQuery);
  }

  startPing() {
    this.timer = setInterval(() => {
      this.webSocket.send(wsPing);
    }, 15000);
  }

  stopPing() {
    clearInterval(this.timer);
  }
}


async function computeQuote(data){
    const respData = JSON.parse(data);

    if(respData.method === 'subscription'){
    
        let quoteObj = {};
    
        const info = respData['params']['data']
        console.log(`${exchangePlatform} info: ${JSON.stringify(info)}`);
    
        if (!info) {
          return
        }
    
        quoteObj.bid_price = info['best_bid_price'];
        quoteObj.ask_price = info['best_ask_price'];
        quoteObj.table = 'quote';
    
        quoteObj.pair = info['instrument_name'];
        quoteObj.timestamp = info['timestamp']
        quoteObj.platform =  exchangePlatform;
    
    
        files.drive(quoteObj, QUOTE_CSV_HEADER);
    }
    
}
module.exports = DeribitWSController
