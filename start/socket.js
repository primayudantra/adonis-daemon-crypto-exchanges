'use strict'

const { sendCustomMessage } = require('../app/Services/Telegram')

/*
|--------------------------------------------------------------------------
| Websocket
|--------------------------------------------------------------------------
|
| This file is used to register websocket channels and start the Ws server.
| Learn more about same in the official documentation.
| https://adonisjs.com/docs/websocket
|
| For middleware, do check `wsKernel.js` file.
|
*/

const BitmexController = use('App/Controllers/Http/BitmexController')
const DeribitController = use('App/Controllers/Http/DeribitController');
const BinanceController = use('App/Controllers/Http/BinanceController');
const FtxController = use('App/Controllers/Http/FtxController')
const BybitController = use('App/Controllers/Http/BybitController');
const HuobiController = use('App/Controllers/Http/HuobiController');

// V2
const DeribitWSController = use('App/Controllers/Http/DeribitWSController');
const FtxWSController = use('App/Controllers/Http/FtxWSController')
const HuobiWSController = use('App/Controllers/Http/HuobiWSController');
const BybitWSController = use('App/Controllers/Http/BybitWSController');

BitmexController.init();
DeribitController.init();
BinanceController.init();
BybitController.init();
FtxController.init();
HuobiController.init();

// V2 WS
FtxWSController.listen();
DeribitWSController.listen();
HuobiWSController.listen();
BybitWSController.listen();

(async function() {
    await sendCustomMessage('Start/Restart All Socket Client');
})();