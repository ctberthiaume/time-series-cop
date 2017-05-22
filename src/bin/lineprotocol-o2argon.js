#!/usr/bin/env node

const fs = require('fs')
  moment = require('moment'),
  _ = require('lodash'),
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

const argv = tscop.cli();

const headers = [ 'time', 'timestamp', 'O2Ar' ];
const types = [ 'time', 'float', 'float' ];

const schema = _.zipObject(headers, types);
const outputSchema = _.zipObject(headers, types);
outputSchema.cruise = 'category';

let count = 0;
let error;
let pipeline = tscop
  .fieldStream({
    stream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    start: 1,
    delimiter:'\t'
  })
  .doto(o => {
    // Only keep relevant fields 0 and 8. Prepend time
    const timestamp = o.fields[0];
    const oxygenArgonRatio = o.fields[7];
    const time = moment.utc({year: 1900})
      .add(parseInt(timestamp) - 2, 'day')
      .add((+timestamp - parseInt(timestamp)) * 24, 'hour');
    o.fields = [ time, timestamp, oxygenArgonRatio ];
  })
  .through(tscop.fieldsToDoc(headers))
  .through(tscop.validateDoc(schema, false))
  .doto(o => {
    o.doc.cruise = argv.cruise;
  })
  .doto(o => {
    count++;
    if (count % 100000 === 0) console.log(count);
  });

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
      console.log(err);
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
      console.log('x ' + err);
    })
    .each(val => outputStream.write(val));
}
