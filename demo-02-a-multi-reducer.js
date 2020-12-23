'use stricr';

require('dotenv').config();
const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');
const mongoDb = require('./MongoDB').singleton();

const { from } = require("rxjs");
const { take, tap, mergeMapTo, mergeMap, reduce } = require('rxjs/operators');

const MESSAGES_TO_PUBLISH = parseInt(process.env.MESSAGES_TO_PUBLISH);

let initTime;
let endTime;
let txs = 0;

mongoDb.start$().pipe(
    tap(() => console.log('MongoDB Started!')),
    mergeMapTo(broker.getMessageListener$(['account'], ['tx'])),
    take(MESSAGES_TO_PUBLISH),
    reduce((acc, val) => {

        if (!initTime) initTime = Date.now();
        endTime = Date.now();
        txs++;

        const { data: { account, type, amount } } = val;
        if (!acc[account]) acc[account] = { amount: 0, account };
        const accountAcc = acc[account];

        if (type === 'DEBIT') accountAcc.amount -= amount;
        else accountAcc.amount += amount;
        return acc;
    }, {}),
    mergeMap(reducedAccounts => from(Object.values(reducedAccounts))),
    mergeMap(({ account, amount }) => mongoDb.incrementAccountBalance$(account, amount))
).subscribe(
    (sentId) => {
    },
    (err) => console.error(err),
    () => {
        const delta = (endTime - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed: TXs=', txs, 'TimeSpan (sec)=', delta, ' TPS(avg)=', tps);
        process.exit(0);
    }
);