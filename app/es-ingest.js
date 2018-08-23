const client = require('node-rest-client-promise').Client({});
const moment = require('moment');

const program = require('commander');
const XXHash = require('xxhash');
const base62 = require("base62/lib/ascii");
const Buffer = require('buffer/').Buffer;
const SEED = 0xa110ca7e;
const SEED2 = 0xca11;
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;

function hash(str) {
    let b = Buffer.from(str, 'utf8');
    let buff64 = XXHash.hash64(b ,SEED);
    let buff32 = XXHash.hash64(b ,SEED2);
    return base62.encode(buff64.readUInt32BE(0)) + base62.encode(buff64.readUInt32BE(4)) + base62.encode(buff32.readUInt32BE(0));

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



function getHistory() {
    return new Promise((resolve, reject) => {
        program.tradePairs[0]
        client.get(`${MARKET_HISTORY_URL}${program.tradePairs[0]}/${999999}`, {}, function (data, response) {
            resolve(data);
        }).on('error', (err) => {
            console.error(err);
            reject(err);
        });
    })
}



Promise.all([getHistory()]).then(results =>{
    console.log(results);
    if(results[0] && results[0].data && results[0].data.length) {
        results[0].data.forEach(r => {


        })
    }
});


/*
function schedule() {
    try {
        performSiteCheck();
    } catch (e) {
        console.error(e);
    }
    setTimeout(schedule, 90 * SECOND);
}

schedule();
*/



console.log(hash(program.tradePairs));