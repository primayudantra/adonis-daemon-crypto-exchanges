'use strict'

const Env = use('Env');
const { sendNotification } = require('../../Services/Telegram')
const _ = require('lodash');
const cron = require('node-schedule');
const moment = require('moment');
const axios = require('axios')
const time = require('../../../Utils/time');
const files = require('../../../Utils/files');
const targetUrlRestAPI = 'https://api.bybit.com/linear/funding-rate/list'
const exchangePlatform = 'BYBIT';
const pairs = ['BTCUSDT','ETHUSDT'];
const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate']

class ByBitController {

    static init(){
      if(Env.get('TYPE') === 'DAEMON'){
        this.fundingRateScheduler();
      }
    }

    static fundingRateScheduler(){

      var rule = new cron.RecurrenceRule();
      rule.minute = 0;

      // Will run every minute
      cron.scheduleJob(rule, async () => {
          try{
              pairs.map(async pair => {
                const data = await getFundingRates(pair)
                if(data.length > 0){
                    compute(data, 'funding')
                }
              })
              
          }catch(e){
              console.log(e)
              // e
          }       
      });
  }
}

async function getFundingRates(pair){
  let dateNow = moment().format('YYYY-MM-DD');

  const url = `${targetUrlRestAPI}?symbol=${pair}&date=${dateNow}%20~%20${dateNow}`;

  try{
    let { data } = await axios.get(url);
    return data.result.data;
  }catch(e){
    console.log(e)
  }
  

}
async function compute(data, type){
    
      if(type === 'funding'){
        const fundingsData = data;
        for(var i = 0; i < fundingsData.length; i++){
            const item = fundingsData[i];
            let obj = {}
            obj.timestamp = moment(item.time).valueOf();
            obj.funding_rate = item.value
            obj.pair = item.symbol;
            obj.platform = exchangePlatform;
            obj.table = 'funding';

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

          }
      }
}
  

module.exports = ByBitController
