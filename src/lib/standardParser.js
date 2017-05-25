const pipeline = require('./pipeline'),
  validation = require('./validation'),
  TimeSeriesCopError = require('./error').TimeSeriesCopError,
  fs = require('fs'),
  _ = require('lodash');

const eregex = new RegExp('^' + validation.errorPrefix);

function parseStandardFile(inputStream, outputStream, delimiter='\t') {
  const inputStream2 = fs.createReadStream(inputStream.path, {encoding: 'utf8'});
  return getStandardHeader(inputStream, delimiter)
    .then(header => validateStandardHeader(header))
    .then(header => parseStandardBody(inputStream2, outputStream, header, delimiter));
}
exports.parseStandardFile = parseStandardFile;

function parseStandardFileToDB(inputStream, host, db, delimiter='\t') {
  const inputStream2 = fs.createReadStream(inputStream.path, {encoding: 'utf8'});
  return getStandardHeader(inputStream, delimiter)
    .then(header => validateStandardHeader(header))
    .then(header => parseStandardBodyToDB(inputStream2, header, host, db, delimiter));
}
exports.parseStandardFileToDB = parseStandardFileToDB;

function getStandardHeader(inputStream, delimiter='\t') {
  let headerKeys = {
    0: 'measurement',
    1: 'cruise',
    2: 'description',
    3: 'columnDescriptions',
    4: 'types',
    5: 'units',
    6: 'headers'
  };
  let rawheader = {
    measurement: { desc: 'measurement' },
    cruise: { desc: 'cruise' },
    description: { desc: 'File description' },
    columnDescriptions: { desc: 'Column descriptions' },
    types: { desc: 'Column types' },
    units: { desc: 'Column units' },
    headers: { desc: 'Column headers' },
  };
  let error;

  return new Promise((resolve, reject) => {
    // Get timestamp and column headers from header section
    pipeline
      .fieldStream({
        instream: inputStream,
        delimiter: delimiter,
        end: 7,
        dropInternalBlank: false,
        dropFinalBlank: false
      })
      .doto(o => rawheader[headerKeys[o.lineIndex]].record = o)
      .stopOnError(err => reject(err))
      .done(() => resolve(rawheader));
    });
}
exports.getStandardHeader = getStandardHeader;

function parseStandardBody(inputStream, outputStream, header, delimiter='\t') {
  return new Promise((resolve, reject) => {
    // Now that we have info from header section, parse data lines
    const schema = _.zipObject(header.headers.data, header.types.data);
    const outputSchema = _.zipObject(header.headers.data, header.types.data);
    outputSchema.cruise = 'category';

    let count = 0;
    pipeline
      .fieldStream({
        instream: inputStream,
        delimiter: delimiter,
        start: 7,
        dropInternalBlank: false,
        dropFinalBlank: true
      })
      .through(pipeline.fieldsToDoc(header.headers.data))
      .through(pipeline.validateDoc(schema, true))
      .doto(o => o.doc.cruise = header.cruise.data)
      .doto(x => count++)
      .through(pipeline.writeDocToLineProtocol({
        measurement: header.measurement.data,
        schema: outputSchema,
        outstream: outputStream
      }))
      .stopOnError(err => reject(err))
      .done(() => resolve('Success. Wrote ' + count + ' points.'));
  });
}
exports.parseStandardBody = parseStandardBody;

function parseStandardBodyToDB(inputStream, header, host, db, delimiter='\t') {
  return new Promise((resolve, reject) => {
    // Now that we have info from header section, parse data lines
    const schema = _.zipObject(header.headers.data, header.types.data);
    const outputSchema = _.zipObject(header.headers.data, header.types.data);
    outputSchema.cruise = 'category';
    let count = 0;
    pipeline
      .fieldStream({
        instream: inputStream,
        delimiter: delimiter,
        start: 7,
        dropInternalBlank: false,
        dropFinalBlank: true
      })
      .through(pipeline.fieldsToDoc(header.headers.data))
      .through(pipeline.validateDoc(schema, true))
      .doto(o => o.doc.cruise = header.cruise.data)
      .doto(x => count++)
      .through(pipeline.writeDocToInfluxDB({
        measurement: header.measurement.data,
        schema: outputSchema,
        host: host,
        database: db
      }))
      .stopOnError(err => reject(err))
      .done(() => resolve('Success. Wrote ' + count + ' points.'));
  });
}
exports.parseStandardBodyToDB = parseStandardBodyToDB;

function validateStandardHeader(origheader) {
  const header = _.cloneDeep(origheader);
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
    throw new TimeSeriesCopError(`${validation.errorPrefix} Incomplete header section(s): ${empties.join(', ')}`);
  }

  // TODO check for allowed measurement name, cruise name through regex and
  // optional konsul lookup if konsul is present.

  // Check that all multi-column header lines are populated and consistent
  // These should be cases where data is an array
  const columnar = _.keys(header).filter(k => _.isArray(header[k].data));

  columnar.forEach(k => {
    const h = header[k];
    h.data.forEach((column, i) => {
      if (column === '') {
        throw new TimeSeriesCopError(`${validation.errorPrefix} ${h.desc} has an empty column on line ${h.record.lineIndex + 1}`);
      }
    });
  });
  const fieldLengths = columnar.map(k => header[k].data.length);
  if (_.uniq(fieldLengths).length > 1) {
    const columnarLines = columnar.map(k => header[k].record.lineIndex + 1);
    throw new TimeSeriesCopError(`${validation.errorPrefix} Lines ${columnarLines.join(',')} must have the same column numbers`);
  }

  if (_.includes(header.headers.data, 'NA')) {
    throw new TimeSeriesCopError(`${validation.errorPrefix} 'NA' is not a valid column header on line ${header.headers.record.lineIndex+1}`);
  }
  if (header.headers.data[0] !== 'time') {
    throw new TimeSeriesCopError(`${validation.errorPrefix} The first headers value on line ${header.headers.record.lineIndex+1} should be 'time'`);
  }
  if (header.types.data[0].toLowerCase() !== 'time') {
    throw new TimeSeriesCopError(`${validation.errorPrefix} The first type value on line ${header.types.record.lineIndex+1} should be 'time'`);
  }

  return header;
}
exports.validateStandardHeader = validateStandardHeader;
