#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('../lib/index'),
  H = require('highland'),
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
// If not defined always skip the header line
startIndex = argv.skip === undefined ? 1 : argv.skip;

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
const outputSchema = {
  cruise: 'category',
  time: 'time',
  beads: 'float',
  picoeuks: 'float',
  prochloro: 'float',
  synecho: 'float',
  unknown: 'float'
};

// Aggregate multiple populations abundances into single points
const popAgg = () => {
  let record;
  let time;
  return (err, o, push, next) => {
    if (err) {
      push(err);
      next();
    }
    else if (o === H.nil) {
      // End of stream, push record if exists
      if (record) {
        push(null, record);
      }
      push(null, o);  // pass along stream end
    } else {
      if (+time !== +o.doc.time) {
        if (time !== undefined) {
          // Push current record
          push(null, record);
        }
        // Start new record
        time = o.doc.time;
        record = {
          doc: {
            cruise: argv.cruise,
            time: time
          }
        };
      }
      // Record adundance
      record.doc[o.doc.pop] = o.doc.abundance;
      record.lineIndex = o.lineIndex;
      record.recordIndex = o.recordIndex;
      next();
    }
  };
};

let outstream;
let count = 0;
if (argv.output) {
  outstream = fs.createWriteStream(argv.output);
}

try {
  let pipeline = tscop.fieldStream({
    instream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: startIndex,
    delimiter:','
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.time = moment(o.doc.timestamp);
    o.doc.cruise = argv.cruise;
  })
  .consume(popAgg())
  .through(tscop.saveData({
    measurement: argv.measurement,
    schema: outputSchema,
    host: argv.host,
    database: argv.db,
    outstream: outstream,
    batchSize: argv.batchSize
  }));  // saveData consumes and ends the stream*/
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
