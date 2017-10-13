#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  H = require('highland'),
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
  'speed_knots': 'float',
  'speed_kmph': 'float'
};

// Aggregate points into speed every five minutes
const speedAgg = () => {
  let prev;
  return (err, o, push, next) => {
    if (err) {
      push(err);
      next();
    } else if (o === H.nil) {
      push(null, o);  // pass along stream end
    } else {
      if (prev !== undefined) {
        let delta_m = moment.duration(prev.doc.time - o.doc.time).asMinutes();
        if (Math.abs(delta_m) > 5) {
          let p1 = [+prev.doc.longitude, +prev.doc.latitude ];
          let p2 = [+o.doc.longitude, +o.doc.latitude ];
          let t1 = prev.doc.time.valueOf();
          let t2 = o.doc.time.valueOf();

          let kmph = tscop.geo2kmph(p1, p2, t1, t2);
          let knots = kmph / 1.852;
          // Set time to halfway between t1 and t2
          let time = moment.utc((prev.doc.time.valueOf() + o.doc.time.valueOf())/2);
          let record = {
            doc: {
              cruise: argv.cruise,
              time: time,
              speed_knots: knots,
              speed_kmph: kmph
            },
            lineIndex: o.lineIndex,
            recordIndex: o.recordIndex
          };
          // Push current record
          push(null, record);
          prev = o;
        }
      } else {
        prev = o;
      }
      next();
    }
  };
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
  .consume(speedAgg())
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
