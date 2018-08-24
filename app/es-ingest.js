const client = require('node-rest-client-promise').Client({});
const moment = require('moment');

const program = require('commander');
const XXHash = require('xxhash');
const base62 = require("base62/lib/ascii");
const Buffer = require('buffer/').Buffer;
const elasticsearch = require('elasticsearch');
const SEED = 0xa110ca7e;
const SEED2 = 0xca117001;
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
let lastRequestedTimestamp = {};
let lastPersistedHash = {};

function ts(){
    return new Date().toLocaleString('it-IT');
}

function hash(str) {
    let b = Buffer.from(str, 'utf8');
    let buff1 = XXHash.hash64(b ,SEED);
    let buff2 = XXHash.hash64(b.reverse() ,SEED2);
    return base62.encode(buff1.readUInt32BE(0)) + base62.encode(buff1.readUInt32BE(4)) + base62.encode(buff2.readUInt32LE(0)) + base62.encode(buff2.readUInt32LE(4));

}



const MARKET_HISTORY_URL = 'https://www.cryptopia.co.nz/api/GetMarketHistory/';


//"LOKI/BTC"
//"GRFT/BTC"
//"ETH/BTC"
program
    .version('0.1.0')
    .usage('[options] <file ...>')
    .option('-t, --tradePairs <trade-pairs>', 'define pairs to monitor', val => val.split(','))
    .option('-c, --esConfig <elasticsearch_config>', 'configuration of elasticsearch connection')
    .parse(process.argv);



const es = new elasticsearch.Client({
    host: program.esConfig,
    log: 'info'
});


function getHistory() {

    return program.tradePairs.map(tradePair => {
        return new Promise((resolve, reject) => {
            if(!lastRequestedTimestamp.hasOwnProperty(tradePair)){
                lastRequestedTimestamp[tradePair] = 0;
            }
            let a = moment(new Date().getTime());
            let b = moment(lastRequestedTimestamp[tradePair]);
            let hoursPast = Math.ceil(moment.duration(a.diff(b)).asHours());
            //console.log(`${ts()} [${tradePair}] ${hoursPast} hours past from last sample`);
            let hours = Math.max(1, hoursPast);

            client.get(`${MARKET_HISTORY_URL}${tradePair}/${hours}`, {}, function (data, response) {
                resolve({tp: tradePair, data: data});

            }).on('error', (err) => {
                console.error(`${ts()}`, err);
                reject(err);
            });
        });
    })
}


function performSiteCheck() {
    Promise.all(getHistory()).then(results => {

        results.forEach(result => {
            let pair = result.tp;
            let res = result.data;
            if(!lastPersistedHash.hasOwnProperty(pair)){
                lastPersistedHash[pair] = {};
            }

            if (res && res.Data && res.Data.length) {
                let bulkBody = [];

                res.Data.sort((a, b) => b.Timestamp - a.Timestamp);
                //filter all records that has been persisted already
                let data = res.Data.map(r => (lastRequestedTimestamp[pair] > r.Timestamp * 1000) ? null : {type: r.Type.toUpperCase(), price: r.Price, amount: r.Amount, ts: (r.Timestamp * 1000)}).filter(r => !!r);


                data.forEach(doc => {
                    let id = hash(JSON.stringify(doc));
                    if (!lastPersistedHash[pair].hasOwnProperty(id)) {
                        bulkBody.push({index: {_index: 'cryptopia_' + pair.toLowerCase(), _type: '_doc', _id: id}});
                        bulkBody.push(doc);
                    }
                    // the document to update
                });

                if (bulkBody.length > 0) {
                    es.bulk({
                        body: bulkBody
                    }, function (err, resp) {
                        let persistedCount = 0;
                        if (err) {
                            console.error(`${ts()}`, err);
                        } else {
                            lastPersistedHash[pair] = {};
                            resp.items.forEach(item => {
                                if (item.index.status < 400) {
                                    lastPersistedHash[pair][item.index._id] = true;
                                    persistedCount++;
                                }

                            });
                            lastRequestedTimestamp[pair] = Math.max(lastRequestedTimestamp[pair], res.Data[0].Timestamp * 1000)
                        }
                        console.log(`${ts()} [${pair}] ${persistedCount} record${persistedCount > 1 ? 's' : ''} persisted`);
                    });
                } else {
                    console.log(`${ts()} [${pair}] no new records to persist`);
                }

            }
        });
    });
}


function schedule() {
    try {
        performSiteCheck();
    } catch (e) {
        console.error(e);
    }
    setTimeout(schedule, 30 * SECOND);
}

schedule();




console.log(`${ts()} - ${hash(program.tradePairs)}`);