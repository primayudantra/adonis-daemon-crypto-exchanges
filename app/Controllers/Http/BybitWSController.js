'use strict'

const Env = use('Env');
const WebSocket = require('ws');
const _ = require('lodash');
const wsUrl = 'wss://stream.bybit.com/realtime_public';
const files = require('../../../Utils/files');
const wsQuery = JSON.stringify({"op": "subscribe", "args": ["orderBookL2_25.BTCUSDT","orderBookL2_25.ETHUSDT"]})
const { sendNotification } = require('../../Services/Telegram')

const wsPing =  JSON.stringify({"op":"ping"})

const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price','ask_price']
const exchangePlatform = 'BYBIT'

let cached_order_book_data = {};

class BybitWSController {
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
        return new BybitWSController(ws, cb);
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

        const data = JSON.parse(message.data)
        if(data.ret_msg === 'pong'){
            console.log(`${exchangePlatform} : Pong ${JSON.stringify(data)}` )
        }else{
            computeQuote(data)
        }
        
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


function _update(updates, pair) {
    console.log(`${exchangePlatform} : Deletions ${JSON.stringify(updates)}` )
  
    if (updates) {
      for (const update of updates) {
        cached_order_book_data[pair].forEach((val, idx, cobdp) => {
          if (val.id === update.id) {
            cobdp[idx] = update;
          }
        });
      }
    }
  }
  
  function _delete(deletions, pair) {
    console.log(`${exchangePlatform} : Deletions ${JSON.stringify(deletions)}` )
    if (deletions.length > 0) {
      cached_order_book_data[pair].forEach((val, idx, cobdp) => {
        for (const deletion of deletions) {
          if (val.id === deletion.id) {
            cobdp = cobdp.filter(item => item.id !== val.id);
          }
        }
      });
    }
  }
  
  function _insert(insertions, pair) {
    console.log(`${exchangePlatform} : Insertions ${JSON.stringify(insertions)}` )
    if (insertions.length > 0) {
      cached_order_book_data[pair] = [...cached_order_book_data[pair], ...insertions];
    }
  }
  
  async function genRecord(pair) {
    let obj = {};
    // BTCUSD
    const data = cached_order_book_data[pair];
  
    // construc obj
    obj.timestamp = data.timestamp;
    obj.table = 'quote';
    obj.platform = exchangePlatform;
    obj.pair = pair;
  
    const bids = data.filter(item => item['side'] === 'Buy').map(item => item['price']);
  
    const asks = data.filter(item => item['side'] === 'Sell').map(item => item['price']);
  
    const best_bid = Math.max(...bids);
    const best_ask = Math.min(...asks);
  
    obj.bid_price = best_bid;
    obj.ask_price = best_ask;
    return obj;
  }
  
  // buy -> bid -> max
  // sell -> ask -> min
  async function computeQuote(data) {
    let obj = {};

    if (data['type'] === 'snapshot') {
      const pair = data['topic'].split('.')[1];
  
      cached_order_book_data[pair] = data['data']['order_book'];
      cached_order_book_data[pair]['timestamp'] = data['timestamp_e6'];
  
      obj = await genRecord(pair);
    }
  
    if (data['type'] === 'delta') {
      const pair = data['topic'].split('.')[1];
  
      //   DELTA SECTION
      const updates = data['data']['update'];
      const insertions = data['data']['insert'];
      const deletions = data['data']['delete'];
  
      _update(updates, pair);
  
      _delete(deletions, pair);
  
      _insert(insertions, pair);
  
      cached_order_book_data[pair]['timestamp'] = data['timestamp_e6'];
  
      obj = await genRecord(pair);
    }
  
    if(_.isEmpty(obj)){
      return;
    }
  
    return files.drive(obj, QUOTE_CSV_HEADER);  
  }
module.exports = BybitWSController
