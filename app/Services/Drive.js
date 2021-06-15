const { Parser } = require('json2csv');
const csv = require('csvtojson')
const Drive = use('Drive');
const moment = require('moment');
const new_line= "\r\n";
const _ = require('lodash');

async function extractObjectValueByKeys(arrayObj, key_name) {
    let arr = []
    arrayObj.map((v, k) => { arr.push(v[key_name]) })

    return _.uniq(arr);
}

async function drive(obj, csv_header){
    let file_name = constructFileName(obj) // "02_02_2020/Bitmex_quote.csv";

    delete obj.table;

    try{
        const exists = await Drive.exists(file_name);

        if(!exists) createFile(file_name, obj, csv_header);
        else appendFile(file_name, obj);

    }catch(e){
        console.log(e)
    }
}

async function createFile(file_name, obj, csv_header){
    let HEADER = csv_header;
    let parser = new Parser({
        HEADER,
        preserveNewLinesInCells: true,
    })

    let new_csv = parser.parse(obj)

    try{
        await Drive.put(file_name, Buffer.from(new_csv + new_line))
    }catch(e){
        console.log(e)
    }

}

async function appendFile(file_name, obj){
    let parser = new Parser({
        header: false,
        preserveNewLinesInCells: true,
    })

    const updated_csv = parser.parse(obj);

    try{
        await Drive.append(file_name, Buffer.from(updated_csv + new_line))
    }catch(e){
        console.log(e)
    }

}

async function getAllTimestampsFromCSV({table, platform}){

    const file_name = constructFileName({table, platform})
    let file = await Drive.get(file_name)

    return csv()
    .fromString(file.toString())
    .then(async (jsonObj)=>{

        const unique_timestamps = await extractObjectValueByKeys(jsonObj, 'timestamp')

        return unique_timestamps;

    })
}

async function getCSVData({table, platform}){
    const file_name = constructFileName({table, platform})
    let file = await Drive.get(file_name)

    return csv().fromString(file.toString()).then(async (jsonObj)=>{
        return jsonObj
    })
}

function constructFileName(obj){
    const date_now = moment().format('DD-MM-YYYY');
    let file_name = `${date_now}/${obj.platform}_${obj.table}.csv`

    return file_name
}

module.exports = {
    drive,
    getCSVData,
    getAllTimestampsFromCSV
}
