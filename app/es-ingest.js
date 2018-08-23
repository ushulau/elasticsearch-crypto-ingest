const client = require('node-rest-client-promise').Client({});
const moment = require('moment');

const program = require('commander');


function range(val) {
    return val.split('..').map(Number);
}

function list(val) {
    return val.split(',');
}

function collect(val, memo) {
    memo.push(val);
    return memo;
}


program
    .version('0.1.0')
    .usage('[options] <file ...>')
    .option('-t, --tradePairs <trade-pairs>', 'define pairs to monitor', list)
    .option('-c, --esConfig <elasticsearch_config>', 'configuration of elasticsearch connection')
    .parse(process.argv);



console.log(program.tradePairs);