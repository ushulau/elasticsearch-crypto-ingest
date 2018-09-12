const program = require('commander');
const XXHash = require('xxhash');
const base62 = require("base62/lib/ascii");
const Buffer = require('buffer/').Buffer;
const elasticsearch = require('elasticsearch');
const SEED = 0xa110ca7e;
const SEED2 = 0xca117001;
const SECOND = 1000;

let bulkBody = [];
let bulkBodyIdLookup = {};
const BigNumber = require('bignumber.js');
const Gdax = require('gdax');
const UPDATED_FLAGS = {};

function ts() {
    return new Date().toLocaleString('it-IT');
}

function hash(str) {
    let b = Buffer.from(str, 'utf8');
    let buff1 = XXHash.hash64(b, SEED);
    let buff2 = XXHash.hash64(b.reverse(), SEED2);
    return base62.encode(buff1.readUInt32BE(0)) + base62.encode(buff1.readUInt32BE(4)) + base62.encode(buff2.readUInt32LE(0)) + base62.encode(buff2.readUInt32LE(4));

}

class MarketPrice {
    constructor(market, date, buyPrice, buySize, sellPrice, sellSize) {
        this.market = market;
        this.date = date;
        this.ts = new Date(date).getTime();
        this.sellPrice = sellPrice;
        this.sellSize = sellSize;
        this.buyPrice = buyPrice;
        this.buySize = buySize;
        this.hash = hash(this.market + this.date + this.sellPrice.toString() + this.sellSize.toString() + this.buyPrice.toString() + this.buySize.toString());
    }

    equals(mp) {
        return mp.hash === this.hash;
    }

    createDoc() {
        return {market: this.market, date: this.date, sellPrice: this.sellPrice.toString(), sellSize: this.sellSize.toString(), buyPrice: this.buyPrice.toString(), buySize: this.buySize.toString(), ts: this.ts};
    }

    toString() {
        return `{market: ${this.market}, date: ${this.date}, sellPrice: ${this.sellPrice.toString()}, sellSize: ${this.sellSize.toString()}, buyPrice: ${this.buyPrice.toString()}, buySize: ${this.buySize.toString()}, ts: ${this.ts}}`;
    }
}

program
    .version('0.1.0')
    .usage('[options] <file ...>')
    .option('-t, --tradePairs <trade-pairs>', 'define pairs to monitor', val => val.split(','))
    .option('-a, --apiCredentials <api-credentials>', 'gdax api credentials', val => eval('(' + val + ')'))
    .option('-c, --esConfig <elasticsearch_config>', 'configuration of elasticsearch connection')
    .option('-i, --indexName <index_name>', 'es index name')
    .parse(process.argv);


const es = new elasticsearch.Client({
    host: program.esConfig,
    log: 'info'
});

const currentMarketBook = {};

if (!program.indexName) {
    program.indexName = `orderbook_coinbase`;
}
if (!program.tradePairs) {
    program.tradePairs = ['LTC-BTC', 'ETH-BTC', 'ETH-USD', 'BTC-USD', 'LTC-USD'];
    console.log(`no products configured will subscribe to -> ${program.tradePairs}`);
} else {
    console.log(`subscribing to following products -> ${program.tradePairs}`);
}


function subscribe() {
    const orderbookSync = new Gdax.OrderbookSync(program.tradePairs,
        'https://api.pro.coinbase.com',
        'wss://ws-feed.pro.coinbase.com',
        program.apiCredentials);

    let synced = 0;
    orderbookSync.on('synced', data => {
        let currentBuy = orderbookSync.books[data]._asks.min();
        let currentSell = orderbookSync.books[data]._bids.max();
        let buyPrice = currentBuy.price;
        let sellPrice = currentSell.price;
        let buySize = currentBuy.orders.reduce((a, val) => {
            return {size: a.size.plus(val.size)}
        }).size;
        let sellSize = currentSell.orders.reduce((a, val) => {
            return {size: a.size.plus(val.size)}
        }).size;
        currentMarketBook[data] = new MarketPrice(data, new Date().toISOString(), buyPrice, buySize, sellPrice, sellSize);
        console.log(`[${data}] is synchronized with current market value and size of -> `, currentMarketBook[data].toString());
        synced += 1;
        UPDATED_FLAGS[data] = true;
        if (synced >= program.tradePairs.length) {
            console.log("▬▬▬▬▬▬▬▬▬▬▬▶ SYNCHRONIZED ◀▬▬▬▬▬▬▬▬▬▬▬")
        }
    });

    orderbookSync.on('error', err => {
        console.error(err)
    });

    orderbookSync.on('close', () => {
        console.log('Socket CLOSED resubscribing....');
        synced = 0;
        subscribe();
    });

    function update(market, date) {
        try {
            let currentBuy = orderbookSync.books[market]._asks.min();
            let currentSell = orderbookSync.books[market]._bids.max();
            let buyPrice = currentBuy.price;
            let sellPrice = currentSell.price;
            let buySize = currentBuy.orders.reduce((a, val) => {
                return {size: a.size.plus(val.size)}
            }).size;

            let sellSize = currentSell.orders.reduce((a, val) => {
                return {size: a.size.plus(val.size)}
            }).size;

            let mp = new MarketPrice(market, date, buyPrice, buySize, sellPrice, sellSize);
            if (!mp.equals(currentMarketBook[market])) {
                //console.log(`[${market}] changed -> `, currentMarketBook[market].toString());
                currentMarketBook[market] = mp;
                bulkBody.push({index: {_index: `${program.indexName}`, _type: '_doc', _id: mp.hash}});
                bulkBody.push(mp.createDoc());
                bulkBodyIdLookup[mp.hash] = mp.market;
                //TODO: DO SOMETHING for instance create a subscription for service


            }
        } catch (e) {
            console.error(e)
        } finally {
            UPDATED_FLAGS[market] = true;
        }

    }


    orderbookSync.on('message', (message) => {
        if (message.type !== "heartbeat" && synced >= program.tradePairs.length) {
            let market = message.product_id;

            if (!UPDATED_FLAGS[market]) return;

            if (message.type === 'done' && !message.price) {
                UPDATED_FLAGS[market] = false;
                setTimeout(() => {
                    update(market, message.time)
                }, 0);
            } else if (message.price) {
                try {
                    let price = new BigNumber(message.price);

                    if (message.side === 'buy' && price.gte(currentMarketBook[market].buyPrice)) {
                        //console.log(`change detected in ${currentMarketBook[market].market}`);
                        UPDATED_FLAGS[market] = false;
                        setTimeout(() => {
                            update(market, message.time)
                        }, 0);
                    } else if (message.side === 'sell' && price.lte(currentMarketBook[market].sellPrice)) {
                        //console.log(`change detected in ${currentMarketBook[market].market}`);
                        UPDATED_FLAGS[market] = false;
                        setTimeout(() => {
                            update(market, message.time)
                        }, 0);
                    }
                } catch (e) {
                    console.error(e);
                }
            }

        }
    })

}

subscribe();
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
                            let pair = bulkBodyIdLookup[item.index._id];
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
                    bulkBodyIdLookup = {};
                } catch (e) {
                    console.error(e);
                }
            });

        }
    } catch (e) {
        console.error(e);
    }
}, 10 * SECOND);