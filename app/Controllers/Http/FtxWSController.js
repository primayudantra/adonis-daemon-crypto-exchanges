'use strict'

const Env = use('Env');
const WebSocket = require('ws');
const { sendNotification } = require('../../Services/Telegram')
const _ = require('lodash');
const files = require('../../../Utils/files');
const wsUrl = 'wss://ftx.com/ws/'

const wsQueryBTCPERP = JSON.stringify({"op": "subscribe","channel": "orderbook", "market": "BTC-PERP"})
const wsQueryETHPERP = JSON.stringify({"op": "subscribe","channel": "orderbook", "market": "ETH-PERP"})
const wsPing =  JSON.stringify({"op": "ping"})

const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price','ask_price']
let cache_order_book_data = {}

const exchangePlatform = 'FTX';

class FTXWSController {
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
        return new FTXWSController(ws, cb);
      }
    } catch (err) {
      console.log(err.message);
      return null;
    }
  }

  startStreaming() {
    this.webSocket.send(wsQueryBTCPERP);
    this.webSocket.send(wsQueryETHPERP);
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
        const data = JSON.parse(message.data);
        if(data.type !== 'pong') {
            computeQuote(data, 'quote')
        }
      }
    } catch (e) {
      console.log(e);
      sendNotification(exchangePlatform, e)
    }
  }

  onerror(err) {
    this.cb(`err: ${err.message}`);
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


async function regenerateCacheOrderBookByPair(pair, order_book){
    let max_ladder = 100;

    if(cache_order_book_data[pair] === undefined){
        cache_order_book_data[pair] = { bids : [], asks :  [] }
        
        cache_order_book_data[pair]['asks'] = _.slice(order_book['asks'], 0, max_ladder)
        cache_order_book_data[pair]['bids'] = _.slice(order_book['bids'], 0, max_ladder)
        
    }else{

        let sorted_bids = _.sortBy(cache_order_book_data[pair]['bids'], [function(o) { return o[0]; }]);
        cache_order_book_data[pair]['bids'] = sorted_bids.slice(0, max_ladder)

        let sorted_asks = _.sortBy(cache_order_book_data[pair]['asks'], [function(o) { return o[0]; }]);
        cache_order_book_data[pair]['asks'] = sorted_asks.slice(0, max_ladder)
    }
}
async function computeQuote(data, table_name){

    let pair = data.market;
    
    if(data.type === 'partial'){
        let order_book = data.data;
        
        let obj = {};

        const bid_price = order_book.bids.map(item => parseFloat(item[0]));
        const ask_price = order_book.asks.map(item => parseFloat(item[0]));

        obj.timestamp = order_book.time;
        obj.pair = pair;
        obj.bid_price = Math.max(...bid_price)
        obj.ask_price = Math.min(...ask_price)
        obj.platform = exchangePlatform
        obj.table = table_name
        
        // cache_order_book_data[pair] = order_book
        await regenerateCacheOrderBookByPair(pair, order_book)

        return files.drive(obj, QUOTE_CSV_HEADER)
        
    }
    if(data.type !== 'partial'){
        // FTX Doesn't has insert and delete;
        // Link : https://docs.ftx.com/#response-format
        if(_.includes(['update'], data.type)){
            
            let delta_data = data.data;
            let delta_asks_data = delta_data.asks;
            let delta_bids_data = delta_data.bids;
            let cache_bids = cache_order_book_data[pair].bids;
            let cache_asks = cache_order_book_data[pair].asks;

            // Delta Asks
            if(delta_asks_data.length > 0){
                for(let x = 0; x < delta_asks_data.length; x++){
                    let ask = delta_asks_data[x];

                    let key = _.findIndex(cache_asks, function(o) { return o[0] == ask[0]; });

                    if(ask[1] === 0){
                        _.remove(cache_asks, function(o) {
                            return o[0] === ask[0];
                        });
                    }else{
                        if(key === -1){
                            console.log(`${exchangePlatform} : Push to cache ask`, ask)
                            cache_order_book_data[pair].asks.push(ask)
                        }else{
                            console.log(`${exchangePlatform} : Update ask`, ask)
                            cache_order_book_data[pair].asks[key] = ask;
                        }
                    }
                    
                }
            }

            // Delta Bids
            if(delta_bids_data.length > 0){
                for(let y = 0; y < delta_bids_data.length; y++){
                    
                    let bid = delta_bids_data[y];
                    let key = _.findIndex(cache_bids, function(o) { return o[0] == bid[0]; });

                    if(bid[1] === 0){
                        _.remove(cache_bids, function(o) {
                            return o[0] === bid[0];
                        });
                    }else{
                        if(key === -1){  
                            console.log(`${exchangePlatform} : Push to cache Bid`, bid)
                            cache_order_book_data[pair].bids.push(bid)
                            
                        }
                        else{
                            console.log(`${exchangePlatform} : Update bid`, bid)
                            cache_order_book_data[pair].bids[key] = bid;
                        }
                    }

                    
                }
            }
            
            await regenerateCacheOrderBookByPair(pair, cache_order_book_data)

            const bid_prices = _.each(cache_order_book_data[pair].bids, item => parseFloat(item[0] ));
            const ask_prices = _.each(cache_order_book_data[pair].asks, item => parseFloat(item[0] ));

            let obj = {};
            
            obj.timestamp = delta_data.time;
            obj.pair = pair;
            obj.bid_price = _.max(bid_prices, function(o) { return o[0]})[0];
            obj.ask_price = _.min(ask_prices, function(o) { return o[0]})[0];
            obj.platform = exchangePlatform
            obj.table = table_name

            return files.drive(obj, QUOTE_CSV_HEADER)
        }

    }
}




module.exports = FTXWSController
