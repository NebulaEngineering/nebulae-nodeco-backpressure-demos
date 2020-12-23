'use stricr';

const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');
const mongoDb = require('./MongoDB').singleton();

const { } = require("rxjs");
const { take, tap, mergeMapTo, mergeMap, reduce } = require('rxjs/operators');

let initTime;
let txs = 0;

mongoDb.start$().pipe(
    tap(() => console.log('MongoDB Started!')),
    mergeMapTo(broker.getMessageListener$(['account'], ['tx'])),
    take(20000),
    reduce((acc, val) => {

        if (!initTime) initTime = Date.now();
        txs++;

        const { data: { account, type, amount } } = val;
        acc.account = account;
        if (type === 'DEBIT') acc.amount += amount;
        else acc.amount -= amount;
        return acc;

    }, { amount: 0 }),
    mergeMap(({ account, amount }) => mongoDb.incrementAccountBalance$(account, amount))
).subscribe(
    (sentId) => {
    },
    (err) => console.error(err),
    () => {
        const delta = (Date.now() - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed! AVG TPS=', tps, 'Delta=', delta);
        process.exit(0);
    }
);