'use strict'

// Market Data Request and Subscription: wss://api.hbdm.com/linear-swap-ws
//
//   Order Push Subscription: wss://api.hbdm.com/linear-swap-notification
//
//   Index Kline Data and Basis Data Subscription: wss://api.hbdm.com/ws_index
//
//   If you fail visiting the two addresses above, you can also visit:
//
//   Market Data Request and Subscription Address: wss://api.btcgateway.pro/linear-swap-ws;
//
//   Order Push Subscription：wss://api.btcgateway.pro/linear-swap-notification
//
//   Index Kline Data and Basis Data Subscription: wss://api.btcgateway.pro/ws_index
//
//   If you have further queries about Huobi USDT Margined Swap order push subscription, please refer to Demo

const Env = use('Env');
const axios = require('axios');
const cron = require('node-schedule');
const { sendNotification } = require('../../Services/Telegram')
const files = require('../../../Utils/files');
const time = require('../../../Utils/time');

const targetRespAPI = 'https://api.hbdm.com/swap-api/v1/swap_funding_rate?contract_code='

const exchangePlatform = 'HUOBI'
const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate']

class HuobiController {
  static init() {
    if(Env.get('TYPE') === 'DAEMON'){
      this.fundingRateScheduler();
    }
  }

  static fundingRateScheduler() {

    var rule = new cron.RecurrenceRule();
    rule.minute = 0;
    
    cron.scheduleJob(rule, async () => {
      try {
        const btc_usd = await getFundingRates('BTC-USD');
        const eth_usd = await getFundingRates('ETH-USD');

        if (btc_usd) {
          await compute(btc_usd, 'funding');
        }

        if (eth_usd) {
          await compute(eth_usd, 'funding');
        }
      } catch (err) {
        await sendNotification(exchangePlatform, err)
        console.log(err);
      }
    });
  }
}

// following huobi naming convention
async function getFundingRates(contract_code) {
  const url = `${targetRespAPI}${contract_code}`;
  try {
    const {data} = await axios.get(url);
    return data
  } catch (err) {
    console.error(`error ${JSON.stringify(err)}`);
  }
}

async function compute(data, type) {

  if (type === 'funding') {
    if (!data) {
      return
    }
    console.log(JSON.stringify(data));
    const fundingObj = {
      timestamp: data['data']['funding_time'],
      pair: data['data']['contract_code'],
      funding_rate: data['data']['funding_rate'],
      platform: exchangePlatform,
      table: 'funding',
    }

    try {
      const unique_keys = await time.getTimestampPairFromCSV({
        table: fundingObj.table,
        platform: fundingObj.platform
      });

      const new_key = fundingObj.timestamp + fundingObj.pair

      if (!unique_keys.includes(new_key)) {
        console.log(`${JSON.stringify(fundingObj)}`);
        await files.drive(fundingObj, FUNDING_CSV_HEADER)
      }
    }catch(e){
      // e means the file doesn't exist
      await files.drive(fundingObj, FUNDING_CSV_HEADER)
    }
  }
}

module.exports = HuobiController
