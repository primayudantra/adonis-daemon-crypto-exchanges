const _ = require('lodash');
const files = require('./files');
const csv = require('csvtojson')
const Drive = use('Drive');

async function getAllTimestampsFromCSV({table, platform}){
  const file_name = files.constructFileName({table, platform})
  let file = await Drive.get(file_name)

  return csv()
    .fromString(file.toString())
    .then(async (jsonObj)=>{

      const unique_timestamps = await files.extractObjectValueByKeys(jsonObj, 'timestamp')

      return unique_timestamps;

    })
}

async function getTimestampPairFromCSV({table, platform}, date = null) {
  const file_name = files.constructFileName({table, platform}, date);
  let file = await Drive.get(file_name);

  return csv()
    .fromString(file.toString())
    .then(async (jsonObj) => {
      return _.uniq(jsonObj.map(item => item['timestamp'] + item['pair']));
    });
}

module.exports = {
  getAllTimestampsFromCSV,
  getTimestampPairFromCSV,
}
