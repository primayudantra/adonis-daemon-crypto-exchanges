'use strict'
const Env = use('Env');
const { sendNotification } = require('../../Services/Telegram')
const cron = require('node-schedule');
const axios = require('axios')
const WebSocketClient = require('websocket').client;
const client = new WebSocketClient();
const files = require('../../../Utils/files');
const time = require('../../../Utils/time');

const targetUrl = 'wss://fstream.binance.com/stream?streams=btcusdt@depth10/ethusdt@depth10@500ms';
const marketLists = ['BTCUSDT', 'ETHUSDT'];

const targetURLRestAPI = `https://fapi.binance.com/fapi/v1`;
const targetRequests = JSON.stringify({
  "jsonrpc": "2.0",
  "id": 7,
  "method": "/public/get_book_summary_by_instrument",
  "params": {
    "instrument_name": "ETH-PERPETUAL",
  }
});

const exchangePlatform = 'BINANCE';
const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price', 'ask_price'];
const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate'];

class BinanceController {
  static init() {
    if(Env.get('TYPE') === 'DAEMON'){
      this.subscribe();
      this.fundingRateScheduler();
    }
  }

  static async subscribe() {
    client.on('connectFailed', async function(error) {
      let e = 'Connect Error: ' + error.toString()
      await sendNotification(exchangePlatform, e)
    });

    client.on('connect', async function(connection) {

      connection.on('error', async function(error) { 
          let e = 'Connection Error: ' + error.toString()
          await sendNotification(exchangePlatform, e)
      });
  
      connection.on('close', async function() {
          let e = 'echo-protocol Connection Closed';
          await sendNotification(exchangePlatform, e)
          reconnect()
      });

      connection.on('message', async function(message) {
        if (message.type === 'utf8') {
          let utf8Data = message.utf8Data;
          compute(utf8Data, 'quote');
        }
      });

      function sendRequest() {
        if (connection.connected) {
          connection.sendUTF(targetRequests);
        }
      }

      sendRequest()
    });

    client.connect(targetUrl, null);

    // self recovery (will reconnect in 30 seconds)
    async function reconnect () {
      setTimeout(function () {
        client.connect(targetUrl, null);
        sendNotification(exchangePlatform, 'Reconnect Socket Client')
      }, 1000 * 30)
    }
  }

  static async fundingRateScheduler(){
    // Will run every 1 hour
    var rule = new cron.RecurrenceRule();
    rule.minute = 0;
    
    cron.scheduleJob(rule, async () => {
        try{
            const data = await getFundingRates()
            if(data.length > 0){
                await compute(data, 'funding')
            }
        }catch(e){
            console.log(e)
            await sendNotification(exchangePlatform, e)
        }
    });
  }
}

async function getFundingRates(){
  const {data} = await axios.get(`${targetURLRestAPI}/fundingRate`);

  let funding_rates_data = [];

  data.map(item => {
      if(marketLists.includes(item.symbol)){
          funding_rates_data.push(item)
      }
  })

  return funding_rates_data;
}

function compute(data, type){
  let quoteObj = {};

  if(type === 'quote'){
    const info = JSON.parse(data)['data'];

    if (!info) {
      return;
    }

    let bid_prices = info['b'].map(item => parseFloat(item[0]));
    let ask_prices = info['a'].map(item => parseFloat(item[0]));

    quoteObj.bid_price = Math.max(...bid_prices);
    quoteObj.ask_price = Math.min(...ask_prices);
    quoteObj.table = 'quote';
    quoteObj.pair = info['s'];
    quoteObj.timestamp = info['E'];
    quoteObj.platform = exchangePlatform;

    console.log(`${exchangePlatform} : `,JSON.stringify(quoteObj))

    return files.drive(quoteObj, QUOTE_CSV_HEADER);
  }

  if(type === 'funding'){

    data.map(async (item) => {
      let obj = {};
      obj.timestamp = item.fundingTime;
      obj.funding_rate = parseFloat(item.fundingRate);
      obj.pair = item.symbol;
      obj.platform = exchangePlatform;
      obj.table = type

      try{
        const unique_keys = await time.getTimestampPairFromCSV({
          table: obj.table,
          platform: obj.platform
        });

        const new_key = obj.timestamp + obj.pair

        if (!unique_keys.includes(new_key)) {
          await files.drive(obj, FUNDING_CSV_HEADER)
          await sendNotification(exchangePlatform, `Store funding Rate Data : ${JSON.stringify(obj)}`)
        }
      }catch(e){
        // e means the file doesn't exist
        await files.drive(obj, FUNDING_CSV_HEADER)
        await sendNotification(exchangePlatform, `Store funding Rate Data : ${JSON.stringify(obj)}`)
      }

    });
  }
}

module.exports = BinanceController

// NOTES : RESPONSE FROM BITMEX BINANCE
// "e": "24hrTicker",  // Event type
  // "E": 123456789,     // Event time
  // "s": "BNBBTC",      // Symbol
  // "p": "0.0015",      // Price change
  // "P": "250.00",      // Price change percent
  // "w": "0.0018",      // Weighted average price
  // "x": "0.0009",      // First trade(F)-1 price (first trade before the 24hr rolling window)
  // "c": "0.0025",      // Last price
  // "Q": "10",          // Last quantity
  // "b": "0.0024",      // Best bid price
  // "B": "10",          // Best bid quantity
  // "a": "0.0026",      // Best ask price
  // "A": "100",         // Best ask quantity
  // "o": "0.0010",      // Open price
  // "h": "0.0025",      // High price
  // "l": "0.0010",      // Low price
  // "v": "10000",       // Total traded base asset volume
  // "q": "18",          // Total traded quote asset volume
  // "O": 0,             // Statistics open time
  // "C": 86400000,      // Statistics close time
  // "F": 0,             // First trade ID
  // "L": 18150,         // Last trade Id
  // "n": 18151          // Total number of trades

  // response
  // {
  //   "stream": "btcusdt@bookTicker",
  //   "data": {
  //     "u": 85007917714,
  //     "s": "BTCUSDT",
  //     "b": "13280.95",
  //     "B": "1.344",
  //     "a": "13280.96",
  //     "A": "12.639",
  //     "T": 1604047219590,
  //     "E": 1604047219595
  //   }
  // };
