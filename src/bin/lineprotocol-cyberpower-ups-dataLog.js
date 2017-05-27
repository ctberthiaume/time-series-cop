#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

let argv;
try {
  argv = tscop.cli();
} catch (e) {
  if (e instanceof TimeSeriesCopError) {
    console.log(`${e.name}: ${e.message}`);
    process.exit(1);
  } else {
    throw e;
  }
}

const headers = [
  'dateString', 'timeString', 'inputMinV', 'inputMaxV', 'inputHz', 'outputV', 'outputHz',
  'load', 'capacity', 'remainingRuntime', 'temp', 'humidity'
];
const types = [
  'text', 'text', 'float', 'float', 'float', 'float', 'float',
  'integer', 'integer', 'integer', 'float', 'integer'
];

const schema = _.zipObject(headers, types);
// Schema for output records. Should specify any properties which
// were added in the transform pipeline. Should only contain properties to be
// included in InfluxDB records.
const outputSchema = _.zipObject(headers, types);
outputSchema.cruise = 'category';
outputSchema.time = 'time';
let outstream
if (argv.output) {
  outstream = fs.createWriteStream(argv.output);
}

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    delimiter:'\t'
  })
  .filter(o => o.fields.length && o.fields[0] !== 'Date') // ignore header line
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment.utc(`${o.doc.dateString}T${o.doc.timeString}Z`);
    o.doc.cruise = argv.cruise;
  })
  .through(tscop.saveData({
    measurement: argv.measurement,
    schema: outputSchema,
    host: argv.host,
    database: argv.db,
    outstream: outstream
  }));  // saveData consumes and ends the stream
}
catch (e) {
  // Catch any errors thrown when creating the pipeline
  // Errors during the pipeline are handled by saveData()
  if (e instanceof TimeSeriesCopError) {
    console.log(`${e.name}: ${e.message}`);
    process.exit(1);
  } else {
    throw e;
  }
}
