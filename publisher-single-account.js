'use stricr';

const { brokerFactory } = require('@nebulae/backend-node-tools').broker;
const broker = brokerFactory('MQTT');

const { range, } = require("rxjs");
const { map, delay, concatMap, mergeMap } = require('rxjs/operators');

let initTime;
let txs = 0;

range(0, 20000).pipe(
    map(i => ({ account: 1010, type: i % 2 == 0 ? 'DEBIT' : 'CREDIT', amount: 1 })),
    //concatMap(tx => broker.send$('account', 'tx', tx).pipe(delay(1)))
    mergeMap(tx => broker.send$('account', 'tx', tx))
).subscribe(
    (sentId) => {
        if (!initTime) initTime = Date.now();
        txs++;
        console.log('sent: ', sentId);
    },
    (err) => console.error(err),
    () => {
        const delta = (Date.now() - initTime) / 1000;
        const tps = txs / delta;
        console.log('Completed! AVG TPS=', tps, 'Delta=', delta);
        process.exit(0);
    }
);