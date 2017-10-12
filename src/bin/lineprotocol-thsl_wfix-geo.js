#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('time-series-cop'),
  TimeSeriesCopError = require('time-series-cop').TimeSeriesCopError;

const argv = tscop.cli();
argv.skip = argv.skip === undefined ? 0 : argv.skip;

const headers = [
  'year', 'julian', 'hour', 'minute', 'second', 'millisecond', 'label', 'latitude',
  'longitude', 'temperature1', 'conductivity', 'salinity', 'temperature2'
];
const types = [
  'integer', 'integer', 'integer', 'integer', 'integer', 'integer', 'text', 'float', 'float',
  'float', 'float', 'float', 'float'
];

// Schema for output records. Should specify any properties which
// were added in the transform pipeline. Should only contain properties to be
// included in InfluxDB records.
const schema = _.zipObject(headers, types);
const outputSchema = {
  'cruise': 'category',
  'time': 'time',
  'latitude': 'float',
  'longitude': 'float'
};

let outstream;
if (argv.output) {
  outstream = fs.createWriteStream(argv.output);
}

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    delimiter: 'whitespace',
    start: argv.skip
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    const time = moment.utc({
      year: o.doc.year,
      hours: o.doc.hour,
      minutes: o.doc.minute,
      seconds: o.doc.second,
      milliseconds: o.doc.milliseconds
    });
    time.dayOfYear(o.doc.julian);
    o.doc.time = time;
    o.doc.cruise = argv.cruise;
  })
  .through(tscop.saveData({
    measurement: argv.measurement,
    schema: outputSchema,
    host: argv.host,
    database: argv.db,
    outstream: outstream,
    batchSize: argv.batchSize,
    windowSize: argv.windowSize
  }));
}
catch (e) {
  if (e instanceof TimeSeriesCopError) {
    console.log(`${e.name}: ${e.message}`);
    process.exit(1);
  } else {
    throw e;
  }
}
