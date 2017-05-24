#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

const argv = tscop.cli();

const headers = 'cruise,file,timestamp,lat,lon,opp_evt_ratio,flow_rate,file_duration,pop,n_count,abundance,fsc_small,chl_small,pe'.split(',');
const types = [
  'text', 'text', 'text', 'text', 'text',
  'float', 'float', 'float', 'category',
  'integer', 'float', 'float', 'float', 'float'
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
    start: 1,
    delimiter:','
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment(o.doc.timestamp);
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
      if (err instanceof TimeSeriesCopError) {
        console.log(err.message);
      } else {
        throw err;
      }
    })
    .done(() => {
      if (!error) console.log('Success. Wrote ' + count + ' points.');
    });
} else {
  const outputStream = fs.createWriteStream(argv.output);
  outputStream.on('finish', () => {
    if (!error) console.log('Success. Wrote ' + count + ' points.');
  });

  pipeline
    .through(tscop.docToLineProtocol(argv.measurement, outputSchema))
    .stopOnError(err => {
      error = err;
      if (err instanceof TimeSeriesCopError) {
        console.log(err.message);
      } else {
        throw err;
      }
    })
    .pipe(outputStream);
}
