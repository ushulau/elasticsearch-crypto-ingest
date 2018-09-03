const program = require('commander');
const XXHash = require('xxhash');
const base62 = require("base62/lib/ascii");
const Buffer = require('buffer/').Buffer;
const elasticsearch = require('elasticsearch');
const SEED = 0xa110ca7e;
const SEED2 = 0xca117001;
const SECOND = 1000;
let lastUpdate = 0;
let bulkBody = [];
const Gdax = require('gdax');

function ts() {
    return new Date().toLocaleString('it-IT');
}

function hash(str) {
    let b = Buffer.from(str, 'utf8');
    let buff1 = XXHash.hash64(b, SEED);
    let buff2 = XXHash.hash64(b.reverse(), SEED2);
    return base62.encode(buff1.readUInt32BE(0)) + base62.encode(buff1.readUInt32BE(4)) + base62.encode(buff2.readUInt32LE(0)) + base62.encode(buff2.readUInt32LE(4));

}

program
    .version('0.2.0')
    .usage('[options] <file ...>')
    .option('-t, --tradePairs <trade-pairs>', 'define pairs to monitor', val => val.split(','))
    .option('-a, --apiCredentials <api-credentials>', 'gdax api credentials', val => eval('(' + val + ')'))
    .option('-c, --esConfig <elasticsearch_config>', 'configuration of elasticsearch connection')
    .parse(process.argv);


const es = new elasticsearch.Client({
    host: program.esConfig,
    log: 'info'
});
if (!program.tradePairs) {
    program.tradePairs = ['LTC-BTC', 'ETH-USD', 'ETH-BTC', 'BTC-USD'];
    console.log(`no products configured will subscribe to -> ${program.tradePairs}`);
} else {
    console.log(`subscribing to following products -> ${program.tradePairs}`);
}


const websocket = new Gdax.WebsocketClient(
    program.tradePairs,
    'wss://ws-feed.pro.coinbase.com',
    program.apiCredentials,
    {channels: ['ticker']}
);


websocket.on('message', data => {

    if (data.length) {
        console.error("data is an array", data);
    } else if (data.type === 'ticker' && data.side) {
        //console.log(data);
        let timestamp = new Date(data.time).getTime();
        let tradePair = data.product_id.toLowerCase().replace('-', '_');
        let doc = {
            price: data.price, size: data.last_size, best_bid: data.best_bid,
            best_ask: data.best_ask, side: data.side, ts: timestamp
        };


        let id = hash(JSON.stringify(doc));
        bulkBody.push({index: {_index: `coinbase_${tradePair}`, _type: '_doc', _id: id}});
        bulkBody.push(doc);

    } else if (new Date().getTime() - lastUpdate >= 30000) {
        lastUpdate = new Date().getTime();
        console.log(`${ts()} no new records to persist. Sequence id -> ${data.sequence}`);
    }
});
websocket.on('error', err => {
    console.error(err)
});
websocket.on('close', () => {
    console.log('CLOSING');
});


setInterval(() => {
    try {
        if (bulkBody.length > 0) {
            es.bulk({
                body: bulkBody
            }, function (err, resp) {
                try {
                    let persistedCount = {};
                    let total = 0;
                    let totalPersisted = 0;
                    if (err) {
                        console.error(`${ts()}`, err);
                    } else {
                        resp.items.forEach(item => {
                            let pair = item.index._index.replace('coinbase_', '');
                            if (!persistedCount.hasOwnProperty(pair)) {
                                persistedCount[pair] = 0;
                            }
                            if (item.index.status < 400) {
                                persistedCount[pair]++;
                                totalPersisted++;
                            }
                            total++;
                        });
                    }
                    console.log(`${ts()} [${totalPersisted}/${total}] persisted => ${JSON.stringify(persistedCount)}`);
                    bulkBody = [];
                } catch (e) {
                    console.error(e);
                }
            });
            lastUpdate = new Date().getTime();
        }
    } catch (e) {
        console.error(e);
    }
}, 5 * SECOND);