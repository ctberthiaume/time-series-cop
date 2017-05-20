const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

const argv = tscop.cli();

const headers = [
  'dateString', 'timeString', 'inputMinV', 'inputMaxV', 'inputHz', 'outputV', 'outputHz',
  'load', 'capacity', 'remainingRuntime', 'temp', 'humidity'
];
const types = [
  'text', 'text', 'float', 'float', 'float', 'float', 'float',
  'integer', 'integer', 'integer', 'float', 'integer'
];

const schema = _.zipObject(headers, types);
const outputSchema = _.zipObject(headers, types);
outputSchema.cruise = 'category';
outputSchema.time = 'time';

tscop
  .fieldStream({
    stream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: 1,
    delimiter:'\t'
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment.utc(`${o.doc.dateString}T${o.doc.timeString}Z`);
    o.doc.cruise = argv.cruise;
  })
  .through(tscop.docToLineProtocol(argv.measurement, outputSchema, false))  // can be in descending order because that's how cyberpower data is
  .stopOnError(err => console.log(err.message))
  .pipe(fs.createWriteStream(argv.output));
