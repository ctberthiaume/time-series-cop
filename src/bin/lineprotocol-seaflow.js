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
    start: 1,
    delimiter:','
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment(o.doc.timestamp);
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
