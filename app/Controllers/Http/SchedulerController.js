'use strict'

const { sendCustomMessage } = require('../../Services/Telegram')
const Helpers = use('Helpers')
const cron = require('node-schedule');
const fs = require('fs');
const S3 = require('../../Services/S3');
const moment = use('moment');
const Drive = use('Drive');
const Env = use('Env');


class SchedulerController {
    static init(){
        if(Env.get('TYPE') === 'DAEMON_S3'){
            this.runStoreAllTemptDataToS3();
        }
    }

    static runStoreAllTemptDataToS3(){
        let rule = new cron.RecurrenceRule();
        rule.minute = 30;
        // Will run every midnight at 1am UTC time
        cron.scheduleJob(rule, async function(){
            await storeAllTempDataToS3();
        });
    }

}

async function storeAllTempDataToS3(){
    const fileOrFolder = fs.readdirSync(Helpers.tmpPath())

    for(let i = 0; i < fileOrFolder.length; i++){
        var folder_path = `${Helpers.tmpPath()}/${fileOrFolder[i]}`

        var is_folder = fs.statSync(folder_path).isDirectory();

        const date_ytd = moment().subtract(1, 'days').format('DD-MM-YYYY');

        if(is_folder && date_ytd === fileOrFolder[i]){

            const files = fs.readdirSync(folder_path)

            for(let x = 0; x < files.length; x++){
                const file_path = `${folder_path}/${files[x]}`

                var buffer_file = await Drive.get(file_path);

                // Insert data to s3
                await S3.insert(files[x], buffer_file)

                // await sendCustomMessage('Data has been inserted to s3 : ' + file_path)
                console.log('Data has been inserted to s3 : ' + file_path);

                await Drive.delete(file_path)
            }
        }
    }
}

module.exports = SchedulerController
