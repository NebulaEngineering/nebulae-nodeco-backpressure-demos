'use stricr';

const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');
const mongoDb = require('./MongoDB').singleton();

const { } = require("rxjs");
const { take, tap, mergeMapTo, mergeMap } = require('rxjs/operators');

let initTime;
let txs = 0;

mongoDb.start$().pipe(
    tap(() => console.log('MongoDB Started!')),
    mergeMapTo(broker.getMessageListener$(['account'], ['tx'])),
    take(20000),
    mergeMap(({ data: { account, type, amount } }) =>
        mongoDb.incrementAccountBalance$(account, type === 'DEBIT' ? amount * (-1) : amount)
    )
).subscribe(
    (sentId) => {
        if (!initTime) initTime = Date.now();
        txs++;
        //console.log('received: ', sentId);
    },
    (err) => console.error(err),
    () => {
        const delta = (Date.now() - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed! AVG TPS=', tps, 'Delta=', delta);
        process.exit(0);
    }
);