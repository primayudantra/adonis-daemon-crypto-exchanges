const { TelegramClient } = require('messaging-api-telegram');
const Env = use('Env')
const moment = use('moment')

const token = Env.get('TELEGRAM_TOKEN');
const chat_id = Env.get('TELEGRAM_CHAT_ID');

const client = new TelegramClient({
    accessToken: token,
  });


async function sendNotification(platform, err_msg){
    let time_now = moment().format();
    let message = 
`
[${Env.get('NODE_ENV').toUpperCase()}]
PLATFORM : ${platform}

${time_now}
--------------------------
${err_msg}
`

    await sendTelegram(message)

}

async function sendCustomMessage(msg){
    let time_now = moment().format();
    let message = 
`
[${Env.get('NODE_ENV').toUpperCase()}]

${time_now}
--------------------------
${msg}
`

    await sendTelegram(message)

}


async function sendTelegram(message){

    if(Env.get('NODE_ENV') === 'testing') return;
    // Listen for any kind of message. There are different kinds of
    // messages.
    await client.sendMessage(chat_id, message, {
        disableWebPagePreview: true,
        disableNotification: true,
      });
}

module.exports = {
    sendNotification,
    sendCustomMessage
}