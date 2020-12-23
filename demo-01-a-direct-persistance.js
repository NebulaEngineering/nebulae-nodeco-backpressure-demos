'use stricr';

require('dotenv').config();
const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');
const mongoDb = require('./MongoDB').singleton();

const { } = require("rxjs");
const { take, tap, mergeMapTo, mergeMap } = require('rxjs/operators');

const MESSAGES_TO_PUBLISH =  parseInt(process.env.MESSAGES_TO_PUBLISH);

let initTime;
let txs = 0;

mongoDb.start$().pipe(
    tap(() => console.log('MongoDB Started!')),
    mergeMapTo(broker.getMessageListener$(['account'], ['tx'])),
    take(MESSAGES_TO_PUBLISH),
    mergeMap(({ data: { account, type, amount } }) =>
        mongoDb.incrementAccountBalance$(account, type === 'DEBIT' ? amount * (-1) : amount)
    )
).subscribe(
    (sentId) => {
        if (!initTime) initTime = Date.now();
        txs++; 
        //console.log('txs: ', txs);
    },
    (err) => console.error(err),
    () => {
        const delta = (Date.now() - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed: TXs=',txs, 'TimeSpan (sec)=',delta,' TPS(avg)=', tps);
        process.exit(0);
    }
);