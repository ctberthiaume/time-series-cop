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

const headers = [ 'time', 'latitude', 'longitude', 'gps_quality', 'satellites', 'altitude', 'geoidal_separation' ]; //'course', 'speed_knots', 'speed_km' ];
const types = [ 'time', 'float', 'float', 'integer', 'integer', 'float', 'float' ];

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

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: startIndex,
    delimiter: 'whitespace'
  })
  .filter(o => o.fields[2].slice(0, 6) === '$GPGGA')
  .doto(o => {
    const timestamp = o.fields[1];
    const time = moment.utc(timestamp, 'YYYY:DDD:HH:mm:ss.SS');
    // Split third field, csv
    const csv = o.fields[2].split(',');
    const latdd = tscop.GGAToDecimalDegrees(csv[2] + csv[3]),
      londd = tscop.GGAToDecimalDegrees(csv[4] + csv[5]),
      qual = csv[6],
      satn = csv[7],
      alt = csv[9],
      geoidalsep = csv[11];
      o.fields = [ time, latdd, londd, qual, satn, alt, geoidalsep ];
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
