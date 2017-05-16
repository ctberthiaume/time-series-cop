const fs = require('fs')
  moment = require('moment'),
  H = require('highland'),
  _ = require('lodash'),
  tscop = require('../lib/index');

const eregex = new RegExp('^' + tscop.errorPrefix);

const argv = tscop.cli();

let headerKeys = {
  0: 'measurement',
  1: 'cruise',
  2: 'description',
  3: 'columnDescriptions',
  4: 'types',
  5: 'units',
  6: 'headers'
};
let header = {
  measurement: { desc: 'measurement' },
  cruise: { desc: 'cruise' },
  description: { desc: 'File description' },
  columnDescriptions: { desc: 'Column descriptions' },
  types: { desc: 'Column data types' },
  units: { desc: 'Column data units' },
  headers: { desc: 'Column headers' },
};
let error;

// Get timestamp and column headers from header section
tscop
  .fieldStream({
    stream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
    delimiter: '\t',
    end: 7,
    dropInternalBlank: false,
    dropFinalBlank: false
  })
  .stopOnError(err => {
    throw err;
  })
  .each(o => {
    header[headerKeys[o.lineIndex]].record = o;
  })
  .done(() => {
    try {
      validateHeader(header);
      // Now that we have info from header section, parse data lines
      const schema = _.zipObject(header.headers.data, header.types.data);
      const outputSchema = _.zipObject(header.headers.data, header.types.data);
      outputSchema.cruise = 'category';

      const outstream = fs.createWriteStream(argv.output);
      outstream.on('finish', () => {
        if (!error) console.log('success');
      });
      tscop
        .fieldStream({
          stream: fs.createReadStream(argv.input, {encoding: 'utf8'}),
          delimiter: '\t',
          start: 7,
          dropInternalBlank: false,
          dropFinalBlank: true
        })
        .through(tscop.fieldsToDoc(header.headers.data))
        .through(tscop.validateDoc(schema, true))
        .doto(o => {
          o.doc.cruise = header.cruise.data;
        })
        .through(tscop.docToLineProtocol(header.measurement.data, outputSchema))
        .stopOnError(err => {
          // Note error here so stream 'finish' doesn't register success.
          // Not sure if necessary but can't hurt.
          error = err;
          if (err.message.match(eregex)) {
            console.log(err.message);
          } else {
            throw err;
          }
        })
        .pipe(outstream);
      } catch (err) {
        // Catch validation errors that occur outside of a highland stream
        // e.g. when through streams are created - fieldstoDoc, validateDoc
        if (err.message.match(eregex)) {
          console.log(err.message);
        } else {
          throw err;
        }
      }
  });


function validateHeader(header) {
  // Make sure we have all header lines

  // Put data in data property to normalize things that should be single
  // text entries versus arrays of text.
  // Get first column for measurement, cruise, descrption
  if (header.measurement.record && header.measurement.record.fields.length) {
    header.measurement.data = header.measurement.record.fields[0];
  }
  if (header.cruise.record && header.cruise.record.fields.length) {
    header.cruise.data = header.cruise.record.fields[0];
  }
  if (header.description.record && header.description.record.fields.length) {
    header.description.data = header.description.record.fields[0];
  }
  // Get all fields for remaining header lines
  if (header.columnDescriptions.record && header.columnDescriptions.record.fields) {
    header.columnDescriptions.data = header.columnDescriptions.record.fields;
  }
  if (header.types.record && header.types.record.fields) {
    header.types.data = header.types.record.fields;
  }
  if (header.units.record && header.units.record.fields) {
    header.units.data = header.units.record.fields;
  }
  if (header.headers.record && header.headers.record.fields) {
    header.headers.data = header.headers.record.fields;
  }

  // Do we have all header sections? Checking data.length looks for either
  // empty strings or empty field arrays.
  const empties = _.keys(header)
    .filter(k => (!header[k].data || !header[k].data.length))
    .map(k => header[k].desc);
  if (empties.length) {
    throw new Error(`${tscop.errorPrefix} Incomplete header section(s): ${empties.join(', ')}`);
  }

  // TODO check for allowed measurement name, cruise name through regex and
  // optional konsul lookup if konsul is present.

  // Check that all multi-column header lines are populated and consistent
  // These should be cases where data is an array
  const columnar = _.keys(header).filter(k => _.isArray(header[k].data));

  columnar.forEach(k => {
    const h = header[k];
    if (h.data.length === 0) {
      throw new Error(`${tscop.errorPrefix} Missing ${h.desc} on line ${h.record.lineIndex + 1}`);
    }
    h.data.forEach((column, i) => {
      if (column === '') {
        throw new Error(`${tscop.errorPrefix} ${h.desc} has an empty column on line ${h.record.lineIndex + 1}`);
      }
    });
  });
  const fieldLengths = columnar.map(k => header[k].data.length);
  if (_.uniq(fieldLengths).length > 1) {
    const columnarLines = columnar.map(k => header[k].record.lineIndex + 1);
    throw new Error(`${tscop.errorPrefix} Lines ${columnarLines.join(',')} must have the same column numbers`);
  }

  if (header.headers.data[0].toLowerCase() !== 'time') {
    throw new Error(`${tscop.errorPrefix} The first headers value on line ${header.headers.record.lineIndex+1} should be 'time'`);
  }
  if (header.types.data[0].toLowerCase() !== 'time') {
    throw new Error(`${tscop.errorPrefix} The first type value on line ${header.types.record.lineIndex+1} should be 'time'`);
  }
}
