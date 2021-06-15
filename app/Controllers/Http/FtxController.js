'use strict'

const Env = use('Env');
const { sendNotification } = require('../../Services/Telegram')
const _ = require('lodash');

const time = require('../../../Utils/time');
const files = require('../../../Utils/files');

const axios = require('axios')
const cron = require('node-schedule');
const moment = require('moment');

const exchangePlatform = 'FTX';
const targetUrl = 'https://ftx.com/api'

const marketLists = ['BTC-PERP', 'ETH-PERP'];

const FUNDING_CSV_HEADER = ['timestamp', 'platform', 'pair', 'funding_rate']


class FtxController {

    static async init(){
        if(Env.get('TYPE') === 'DAEMON'){
            this.fundingRateScheduler()
        }
    }

    static async fundingRateScheduler(){
        var rule = new cron.RecurrenceRule();
        rule.minute = 0;

        // Will run every hour
        cron.scheduleJob(rule, async function(){
            try{
                const data = await getFundingRates()
                if(data.length > 0){
                    compute(data, 'funding')
                }
            }catch(e){
                await sendNotification(exchangePlatform, e)
                console.log(e)
            }       
        });
    }
}

async function getFundingRates(){
    const url = `${targetUrl}/funding_rates`;

    const {data} = await axios.get(url);

    let markets_data = [];

    data.result.map( item => {
        const { future } = item;
        if(marketLists.includes(future)){
            markets_data.push(item)
        }
    })

    return markets_data;
}


async function compute(data, type){

    if(type === 'funding'){

        const fundingsData = data;
        for(var i = 0; i < fundingsData.length; i++){
            const item = fundingsData[i];

            const dateDay = moment(item.time).utc().format('YYYY-MM-DD')
            const today = moment().utc().format('YYYY-MM-DD')

            if(dateDay === today){
                let obj = {};
                obj.timestamp = moment(item.time).valueOf();
                obj.funding_rate = item.rate;
                obj.pair = item.future;
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
                    }

                }catch(e){
                    // e means the file doesn't exist
                    await files.drive(obj, FUNDING_CSV_HEADER)
                }  
            }

              
        }    
    }    
}    


module.exports = FtxController
