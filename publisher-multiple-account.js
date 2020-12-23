'use stricr';

require('dotenv').config();
const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');

const { range, } = require("rxjs");
const { map, delay, concatMap, mergeMap, tap } = require('rxjs/operators');

const MESSAGES_TO_PUBLISH = parseInt(process.env.MESSAGES_TO_PUBLISH);
const ACCOUNTS = [2020, 3030, 4040, 5050, 6060, 7070, 8080, 9090];

let initTime;
let endTime;
let txs = 0;

range(0, MESSAGES_TO_PUBLISH).pipe(
    map(i => {
        const account = ACCOUNTS[i % (ACCOUNTS.length)];
        return { account, type: i % 2 == 0 ? 'DEBIT' : 'CREDIT', amount: i % 2 == 0 ? 1 : 2 };
    }),
    //concatMap(tx => broker.send$('account', 'tx', tx))
    //concatMap(tx => broker.send$('account', 'tx', tx).pipe(delay(1)))
    mergeMap(tx => broker.send$('account', 'tx', tx)),
    tap(() => {
        if (!initTime) initTime = Date.now();
        endTime = Date.now();
        txs++;
    }),
).subscribe(
    (sentId) => {
        //console.log('sent: ', sentId);
    },
    (err) => console.error(err),
    () => {
        const delta = (endTime - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed: TXs=', txs, 'TimeSpan (sec)=', delta, ' TPS(avg)=', tps);
        process.exit(0);
    }
);