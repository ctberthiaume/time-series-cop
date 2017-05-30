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

const headers = [ 'time', 'sstemp', 'conductivity', 'salinity', 'sound_velocity', 'sstemp_bow' ];
const types = [ 'time', 'float', 'float', 'float', 'float', 'float' ];

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
// tsgraw	2017:150:04:54:47.8285	t1= 25.3496, c1= 5.10864, s= 33.2747, sv=1532.951, t2= 25.1663

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: startIndex,
    delimiter: 'whitespace'
  })
  .filter(o => { return o.fields.length === 11 })
  .doto(o => {
    const timestamp = o.fields[1];
    const time = moment.utc(timestamp, 'YYYY:DDD:HH:mm:ss.SS');
    const len = o.fields.length.toString()
    const sstemp = o.fields[3].substr(0, o.fields[3].length-1),
      conductivity = o.fields[5].substr(0, o.fields[5].length-1),
      salinity = o.fields[7].substr(0, o.fields[7].length-1),
      sound_velocity = o.fields[8].substr(3, o.fields[8].length-1),
      sstemp_bow = o.fields[10];
    o.fields = [ time, sstemp, conductivity, salinity, sound_velocity, sstemp_bow ];
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
