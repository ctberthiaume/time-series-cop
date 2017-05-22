/**
 * Process command-line arguments for a text file to Line Protocol script.
 * @returns {Object} yargs argv object
 */
function baseCli() {
  const argv = require('yargs')
    .usage('Usage: $0 [options]')
    .example('$0 -c KOK1606 -m seaflow -i seaflow.csv -d mydb -H localhost')
    .describe('c', 'Cruise name for Line Protocol tags')
    .alias('c', 'cruise')
    .nargs('c', 1)
    .describe('m', 'InfluxDB measurement name')
    .alias('m', 'measurement')
    .nargs('m', 1)
    .describe('i', 'Input file path')
    .alias('i', 'input')
    .nargs('i', 1)
    .group(['cruise', 'measurement', 'input'], 'Common Options')
    .describe('o', 'Output file path. Incompatible with -d or -H.')
    .alias('o', 'output')
    .nargs('o', 1)
    .conflicts('output', 'db')
    .group('output', 'Line protocol File Output Options')
    .describe('host', 'InfluxDB hostname. Incompatible with -o.')
    .alias('H', 'host')
    .nargs('H', 1)
    .describe('d', 'InfluxDB database name. Incompatible with -o.')
    .alias('d', 'db')
    .nargs('d', 1)
    .implies('db', 'host')
    .implies('host', 'db')
    .group(['host', 'db'], 'InfluxDB Write Options')
  return argv;
}

function standardCli() {
  return baseCli()
    .demandOption(['i'])
    .argv;
}
exports.standardCli = standardCli;

function cli() {
  return baseCli()
    .demandOption(['i', 'm', 'c'])
    .argv;
}
exports.cli = cli;
