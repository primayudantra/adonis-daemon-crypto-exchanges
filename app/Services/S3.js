const Drive = use('Drive');
const moment = require('moment');
const bucket_name = 'Exchanges'

async function insert(file_name, buffer_file){
    let date_subtract_1_day = moment().subtract(1, 'days').format('DD-MM-YYYY');
    let file_s3_name = `${bucket_name}/${date_subtract_1_day}/${file_name}`

    await Drive.disk('s3').put(file_s3_name, buffer_file)
}


module.exports = {
    insert
}