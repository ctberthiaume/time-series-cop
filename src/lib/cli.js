/**
 * Process command-line arguments for a text file to Line Protocol script.
 * @returns {Object} yargs argv object
 */
function standardCli() {
  const argv = require('yargs')
    .usage('Usage: $0 [options]')
    .example('$0 -c KOK1606 -m seaflow -f seaflow.csv')
    .describe('c', 'Cruise name for Line Protocol tags')
    .alias('c', 'cruise')
    .nargs('c', 1)
    .describe('m', 'InfluxDB measurement name')
    .alias('m', 'measurement')
    .nargs('m', 1)
    .describe('i', 'Input file path. Will be converted to a node readable stream.')
    .alias('i', 'input')
    .nargs('i', 1)
    .describe('t', 'Create a tailing stream of input file contents (always-tail npm module)')
    .alias('t', 'tail')
    .boolean('t')
    .describe('o', 'Output file path. Will be converted to a node writable stream. - for stdout.')
    .alias('o', 'output')
    .nargs('o', 1)
    .default('o', '-')
    .demandOption(['i'])
    .argv;
  return argv;
}
exports.standardCli = standardCli;

function cli() {
  const argv = require('yargs')
    .usage('Usage: $0 [options]')
    .example('$0 -c KOK1606 -m seaflow -f seaflow.csv')
    .describe('c', 'Cruise name for Line Protocol tags')
    .alias('c', 'cruise')
    .nargs('c', 1)
    .describe('m', 'InfluxDB measurement name')
    .alias('m', 'measurement')
    .nargs('m', 1)
    .describe('i', 'Input file path. Will be converted to a node readable stream.')
    .alias('i', 'input')
    .nargs('i', 1)
    .describe('t', 'Create a tailing stream of input file contents (always-tail npm module)')
    .alias('t', 'tail')
    .boolean('t')
    .describe('o', 'Output file path. Will be converted to a node writable stream. - for stdout.')
    .alias('o', 'output')
    .nargs('o', 1)
    .default('o', '-')
    .demandOption(['i', 'm', 'c'])
    .argv;
  return argv;
}
exports.cli = cli;
