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
// Skip header line
startIndex = argv.skip === undefined ? 1 : argv.skip;

const headers = [ 'time', 'par' ];
const types = [ 'time', 'float' ];

const schema = _.zipObject(headers, types);
// Schema for output records. Should specify any properties which
// were added in the transform pipeline. Should only contain properties to be
// included in InfluxDB records.
const outputSchema = _.zipObject(headers, types);
outputSchema.cruise = 'category';
let outstream
if (argv.output) {
  outstream = fs.createWriteStream(argv.output);
}

// Sample of line format:
// 2016-04-20 00:04:37,1939.9

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: startIndex,
    delimiter: ','
  })
  .doto(o => {
    const timestamp = o.fields[0];
    const time = moment.utc(timestamp, 'YYYY-MM-DD HH:mm:ss');
    const par = o.fields[1];
    o.fields = [ time, par ];
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.cruise = argv.cruise;
  })
  .through(tscop.saveData({
    measurement: argv.measurement,
    schema: outputSchema,
    host: argv.host,
    database: argv.db,
    outstream: outstream,
    batchSize: argv.batchSize
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
