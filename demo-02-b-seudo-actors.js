'use stricr';

require('dotenv').config();
const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');
const mongoDb = require('./MongoDB').singleton();

const { from, forkJoin, of } = require("rxjs");
const { take, tap, mergeMapTo, mergeMap, reduce, groupBy, scan, sampleTime } = require('rxjs/operators');

const MESSAGES_TO_PUBLISH = parseInt(process.env.MESSAGES_TO_PUBLISH);

let initTime;
let endTime;
let txs = 0;

mongoDb.start$().pipe(
    tap(() => console.log('MongoDB Started!')),
    mergeMapTo(broker.getMessageListener$(['account'], ['tx'])),
    groupBy(msg => msg.data.account),
    mergeMap(msgGroup$ => msgGroup$.pipe(
        scan((acc, val) => {
            if (!initTime) initTime = Date.now();
            txs++;
            endTime = Date.now();

            const { data: { account, type, amount } } = val;
            if (type === 'DEBIT') acc.amount -= amount;
            else acc.amount += amount;
            return acc;
        }, { amount: 0, account: msgGroup$.key }),
        sampleTime(5000),
        mergeMap(({ account, amount }) =>
            forkJoin([
                mongoDb.incrementAccountBalance$(account, amount),
                of({ account, amount })
            ])
        ),
    ))


).subscribe(
    ([_,{ account, amount }]) => {
        console.log('Flushed', account, amount);
    },
    (err) => console.error(err),
    () => {
        const delta = (endTime - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed: TXs=', txs, 'TimeSpan (sec)=', delta, ' TPS(avg)=', tps);
        process.exit(0);
    }
);