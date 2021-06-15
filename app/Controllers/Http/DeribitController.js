'use strict'

const Env = use('Env');
const { sendNotification } = require('../../Services/Telegram')
const files = require('../../../Utils/files');
const time = require('../../../Utils/time');
const axios = require('axios');
const cron = require('node-schedule');
const moment = require('moment');


// end_timestamp
// start_timestamp
// instrument_name
const targetURLRestAPI = 'https://www.deribit.com/api/v2/public/get_funding_rate_history?';

const exchangePlatform = 'DERIBIT'
const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate']


class DeribitController {
  static init() {
    if(Env.get('TYPE') === 'DAEMON'){
      this.fundingRateScheduler();
      this.reRunFundingRateScheduler();
    }
  }

  static async fundingRateScheduler() {
    let rule = new cron.RecurrenceRule();
    rule.hour = 0;
    rule.minute = 15;

    // Scheduler: will run every 00.30 AM UTC TIME
    cron.scheduleJob(rule, async () => {
      await sendNotification(exchangePlatform, 'Collecting funding rate data has been started')
      try {
        const btcPerpetualData = await getFundingRates('BTC-PERPETUAL');
        const ethPerpetualData = await getFundingRates('ETH-PERPETUAL');

        if (btcPerpetualData) {
          await compute(btcPerpetualData, 'funding');
        }

        if (ethPerpetualData) {
          await compute(ethPerpetualData, 'funding');
        }

      } catch (err) {
        await sendNotification(exchangePlatform, err)
        console.log(err);
      }
    });
  }

  static async reRunFundingRateScheduler() {
    let rule = new cron.RecurrenceRule();
    rule.hour = 0;
    rule.minute = 20;

    // Scheduler: will run every 00.40 AM UTC TIME
    cron.scheduleJob(rule, async () => {
      try {
        const btcPerpetualData = await getFundingRates('BTC-PERPETUAL');
        const ethPerpetualData = await getFundingRates('ETH-PERPETUAL');

        if (btcPerpetualData) {
          await compute(btcPerpetualData, 'funding');
        }

        if (ethPerpetualData) {
          await compute(ethPerpetualData, 'funding');
        }

      } catch (err) {
        await sendNotification(exchangePlatform, err)
        console.log(err);
      }
    });
  }

}

async function getFundingRates(instrument_name){
  const start_timestamp = moment().subtract(1, 'days').utc().startOf('day').format('x')
  const end_timestamp = moment().utc().startOf('day').format('x')

  const url = `${targetURLRestAPI}end_timestamp=${end_timestamp}&start_timestamp=${start_timestamp}&instrument_name=${instrument_name}`

  await sendNotification(exchangePlatform, `Getting Funding Rate Data : ${url}`)

  try {
    const {data} = await axios.get(url);
    return {...data,instrument_name};
  } catch (err) {
    await sendNotification(exchangePlatform, err)
    console.error(`error ${JSON.stringify(err)}`);
  }
}

async function compute(data, type){
  const date = moment().subtract(1, 'days').format('DD-MM-YYYY');

  if (type === 'funding') {
    if (!data) {
      return;
    }

    const fundingRatesData = data.result;

    for(let i = 0; i < fundingRatesData.length; i++){

      const fundingRate = fundingRatesData[i];
      const fundingObj = {
        timestamp:  fundingRate['timestamp'],
        pair: data['instrument_name'],
        funding_rate: fundingRate['interest_8h'],
        platform: exchangePlatform,
        table: 'funding'
      }

      try{  
        const unique_keys = await time.getTimestampPairFromCSV({
          table: fundingObj.table,
          platform: fundingObj.platform
        }, date);
        
        const new_key = fundingObj.timestamp + fundingObj.pair
  
        if (!unique_keys.includes(new_key)) {
          await files.drive(fundingObj, FUNDING_CSV_HEADER, date)
        }
        
      }catch(e){
        // e means the file doesn't exist
        await files.drive(fundingObj, FUNDING_CSV_HEADER, date)
      }
    }
  }
}

module.exports = DeribitController
