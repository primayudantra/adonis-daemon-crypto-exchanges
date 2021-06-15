'use strict'
const Env = use('Env');
const WebSocket = require('ws');
const BitMEXClient = require('bitmex-realtime-api');
const files = require('../../../Utils/files');
const time = require('../../../Utils/time');
const { sendNotification } = require('../../Services/Telegram');

const exchangePlatform = 'BITMEX';

const QUOTE_CSV_HEADER = ['timestamp', 'platform', 'pair', 'bid_price','ask_price']
const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate']
const currencies = ['ETHUSD', 'XBTUSD']
const bitmexClientConfig = {
    testnet: false, // set `true` to connect to the testnet site (testnet.bitmex.com)
    maxTableLen: 1  // the maximum number of table elements to keep in memory (FIFO queue)
}
const bitmexWsURL = 'wss://www.bitmex.com/realtime';
const bitmexClient = new BitMEXClient(bitmexClientConfig);

class BitmexController {
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
          ws = new WebSocket(bitmexWsURL);
          return new BitmexController(ws, cb);
        } catch (err) {
          console.log(err.message);
          return null;
        }
    }

    onopen() {
        this.isConnected = true;
        this.startPing();
    }

    onclose() {
        let e = 'echo-protocol Connection Closed';  
        this.isConnected = false;
        sendNotification(exchangePlatform, e)
    }

    onmessage(message) {
        try {
          if (message) {
            console.log(exchangePlatform + " : " + message.data)
          }
        } catch (e) {
          console.log(e);
          sendNotification(exchangePlatform, e)
        }
      }

    startPing() {
        this.timer = setInterval(() => {
          this.webSocket.send('ping');
        }, 10000);
      }
    
      stopPing() {
        clearInterval(this.timer);
      }
    

    static init(){
      if(Env.get('TYPE') === 'DAEMON'){
        this.subscribeQuote();
        this.subscribeFunding();
        this.listen()
      }
    }

    static async subscribeQuote(){
        for(var i = 0; i < currencies.length; i++){
            bitmexClient.addStream(currencies[i], 'quote', function (data, symbol, tableName) {
                console.log(exchangePlatform + " : " + JSON.stringify(data[0]))
                compute(data[0], tableName, symbol)
            });
        }    
    }

    static async subscribeFunding(){
        for(var i = 0; i < currencies.length; i++){
            bitmexClient.addStream(currencies[i], 'funding', function (data, symbol, tableName) {
                console.log(exchangePlatform + " Funding : " + JSON.stringify(data[0]))
                compute(data[0], tableName, symbol)
            });
        }    
    }
}

async function compute(data, table, symbol){
    let obj = {}

    if(table === 'quote'){
        obj.timestamp = new Date(data.timestamp).getTime();
        obj.bid_price = data.bidPrice;
        obj.ask_price = data.askPrice;
        obj.pair = symbol;
        obj.platform = exchangePlatform;
        obj.table = table;

        files.drive(obj, QUOTE_CSV_HEADER)
    }

    if(table === 'funding'){
        obj.timestamp = new Date(data.timestamp).getTime();
        obj.funding_rate = data.fundingRate;
        obj.pair = data.symbol;
        obj.platform = exchangePlatform;
        obj.table = table;

        try{
            const unique_keys = await time.getTimestampPairFromCSV({
              table: obj.table,
              platform: obj.platform
            });
        
            const new_key = obj.timestamp + obj.pair
        
            if (!unique_keys.includes(new_key)) {
                files.drive(obj, FUNDING_CSV_HEADER)
            }
            
          }catch(e){
            sendNotification(exchangePlatform, "Funding : " + JSON.stringify(obj))
            // e means the file doesn't exist
            files.drive(obj, FUNDING_CSV_HEADER)
          }
        
    }
}

module.exports = BitmexController
