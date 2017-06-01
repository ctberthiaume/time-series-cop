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
startIndex = argv.skip === undefined ? 0 : argv.skip;

const headers = [ 'time', 'chlorophyll', 'scattering', 'cdom' ];
const types = [ 'time', 'float', 'float', 'float' ];

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
// ecotriplet  2017-05-31T00:00:14.1680  99/99/99  99:99:99  695  52  700  879  460  64  521

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: startIndex,
    delimiter: 'whitespace'
  })
  .doto(o => {
    const timestamp = o.fields[1];
    const time = moment.utc(timestamp);
    // Apply calibrations to raw Voltages and write back as strings
    const chlorophyll = (0.0072 * (parseInt(o.fields[6]) - 42)).toString();
    const scattering = (1.611e-6 * (parseInt(o.fields[8]) - 42)).toString();
    const cdom = (0.0901 * (parseInt(o.fields[10]) - 34)).toString();
    o.fields = [ time, chlorophyll, scattering, cdom ];
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
