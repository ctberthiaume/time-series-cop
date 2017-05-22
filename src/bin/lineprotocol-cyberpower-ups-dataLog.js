#!/usr/bin/env node

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


let count = 0;
let error;
let pipeline = tscop
  .fieldStream({
    stream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    delimiter:'\t'
  })
  .filter(o => o.fields.length && o.fields[0] !== 'Date') // ignore header line
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment.utc(`${o.doc.dateString}T${o.doc.timeString}Z`);
    o.doc.cruise = argv.cruise;
  })
  .doto(o => count++);

if (argv.db && argv.host) {
  pipeline
    .through(tscop.prepDocForInfluxDB(argv.measurement, outputSchema))
    .through(tscop.writeDocsToInfluxDB({
      measurement: argv.measurement,
      schema: outputSchema,
      host: argv.host,
      database: argv.db
    }))
    .stopOnError(err => {
      error = err;
      console.log(err);
    })
    .done(() => {
      if (!error) console.log('Success. Wrote ' + count + ' points.');
    });
} else {
  const outputStream = fs.createWriteStream(argv.output);
  outputStream.on('finish', () => {
    console.log('Success. Wrote ' + count + ' points.');
  });

  pipeline
    .through(tscop.docToLineProtocol(argv.measurement, outputSchema))
    .stopOnError(err => console.log(err))
    .pipe(outputStream);
}
