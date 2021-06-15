## Introduction
This application is using adonisjs, it has two types of applications, `DAEMON` and `DAEMON_S3`

## DAEMON 
Daemon is background processing to getting REALTIME data (Bid and Ask) from crypto exchanges (Binance, Deribit, Bitmex, FTX, HUOBI, and Bybit) and store the data to csv.

## DAEMON_S3
Daemon S3 is background processing to store the data into S3.


## Setup and Installation

```
npm install
cp .env.example .env

# Run Daemon Processing
TYPE=DAEMON node server.js


# Run Daemon S3
TYPE=DAEMON_S3 node server.js
```
![YOW](demo.gif)




---

## Notes

AdonisJS 4.0 Documentation [URL](https://legacy.adonisjs.com/docs/4.1/upgrade-guide)

