const fs = require('fs');
const H = require('highland');
const CSV = require('csv-string');
const _ = require('lodash');
const eolFix = require('eol-fix-stream');
const Influx = require('influx');
const JsonInfluxDbStream = require('json-to-influxdb-line').JsonInfluxDbStream;
const validation = require('./validation');
const TimeSeriesCopError = require('./error').TimeSeriesCopError;

/**
 * Create a Highland stream to split a text into lines. Line endings are
 * normalized before splitting and stripped from final text. Produces objects
 * with { text: <line text>, lineIndex: <line index> }.
 * text
 * @param {Object} [stream=null] Input to Highland stream constructor. May be
 * Node readable stream, array, EventEmitter, Promise, etc. See Highland
 * docs.
 * @param {number} [start=0] Index of line to begin processing (inclusive)
 * @param {number} [end=Infinty] Index of line stop processing (exclusive)
 * @param {boolean} [dropInternalBlank=true] Drop empty lines between non-empty
 * lines.
 * @param {boolean} [dropFinalBlank=true] Drop empty lines at the end of the
 * stream.
 * @example
 * // Add text to each line of readable stream
 * lineStream({stream: readable})
 *   .map(line => line.text + 'NEWTEXT')
 *   .each(H.log);
 * @returns {Object} Highland stream
 */
function lineStream({
  stream=null,
  start=0,
  end=Infinity,
  dropInternalBlank=true,
  dropFinalBlank=true
} = {}) {
  let i = 0;
  return H(stream)  // make a highland stream
    .through(eolFix())  // normalize line endings
    .split()  // line splitter
    .map(line => ({ text: line, lineIndex: i++ }))
    .slice(start, end)  // get selected lines only
    .consume(dropBlanks({dropInternalBlank, dropFinalBlank}));
}
exports.lineStream = lineStream;

/**
 * Create a Highland stream to split each line of a text stream into an array of
 * fields. Input values should be objects that look like this:
 * { text: <line text>, lineIndex: <line index> } (e.g. made by lineStream). Output
 * objects will be the same with additional 'fields' and 'recordIndex' properties.
 * @param {Object} [stream=null] Input to Highland stream constructor. May be
 * Node readable stream, array, EventEmitter, Promise, etc. See Highland
 * docs.
 * @param {number} [start=0] Index of line to begin processing (inclusive)
 * @param {number} [end=Infinty] Index of line stop processing (exclusive)
 * @param {boolean} [dropInternalBlank=true] Drop empty lines between non-empty
 * lines.
 * @param {boolean} [dropFinalBlank=true] Drop empty lines at the end of the
 * stream.
 * @param {string} [delimiter='\t'] String to use as field separator, or
 * special string 'whitespace' to split on whitespace.
 * @example
 * // Print number of fields in each line
 * fieldStream({stream: readable})
 *   .map(line => line.fields.length.toString())
 *   .each(H.log);
 * @returns {Object} Highland stream
 */
function fieldStream({
  stream=null,
  start=0,
  end=Infinity,
  dropInternalBlank=true,
  dropFinalBlank=true,
  delimiter='\t'
} = {}) {
  let i = 0;
  let pipe = lineStream({stream, start, end, dropInternalBlank, dropFinalBlank});
  // Turn line text into array of field values
  if (delimiter === 'whitespace') {
    pipe = pipe.doto(o => {
      if (o.text === '') {
        o.fields = [];
      } else {
        o.fields = splitOnWhitespace(o.text);
      }
    });
  } else {
    pipe = pipe.doto(o => {
      if (o.text === '') {
        o.fields = [];
      } else {
        o.fields = CSV.parse(o.text, delimiter)[0];
      }
    });
  }
  pipe = pipe.doto(o => o.recordIndex = i++);
  return pipe;
}
exports.fieldStream = fieldStream;

/**
 * Create a Highland stream transform function to convert an array of fields
 * into an object. The fields array should be in each input object under
 * 'fields'. A new property 'doc' will be added to the object in transit.
 * @param {string[]} headers Names of input array fields in same order. Will
 * become properties of output objects.
 * @returns {Object} Highland stream transform function for use with through()
 */
function fieldsToDoc(headers) {
  return (stream) => {
    return stream
      .doto(o => {
        if (o.fields.length !== headers.length) {
          throw new TimeSeriesCopError(`Column count (${o.fields.length}) does not match header count (${headers.length}) on line ${o.lineIndex + 1}`);
        }
        o.doc = _.zipObject(headers, o.fields);
      });
  };
}
exports.fieldsToDoc = fieldsToDoc;

/**
 * Create a Highland stream transform function to validate an object based on
 * a supplied schema. The original document before validation will be moved to
 * 'origdoc', and the new validated document will replace 'doc'.
 * @param {object} schema Type associations for properties to validate. Values
 * be [category,text,float,integer,boolean,time].
 * @param {boolean} [strict=false] Use the strict form of validation
 * @returns {Object} Highland stream transform function for use with through()
 */
function validateDoc(schema, strict) {
  const validator = strict ? validation.validators.strict : validation.validators.lax;

  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new TimeSeriesCopError(`Invalid type '${schemaValidation.error}'`);
  }
  schema = schemaValidation.schema;  // set validated, case-normalized schema

  return (stream) => {
    return stream
      .doto(o => {
        o.origDoc = o.doc;
        o.doc = {};
        Object.keys(o.origDoc).forEach(k => {
          if (schema[k]) {
            if (validator[schema[k]] === undefined) {
              throw new TimeSeriesCopError(`Invalid type '${schema[k]}'`);
            }
            const v = validator[schema[k]](o.origDoc[k]);
            if (v.error) {
              throw new TimeSeriesCopError(`${v.error} on line ${o.lineIndex + 1}. column=${k}, value=${o.origDoc[k]}, type=${schema[k]}`);
            }
            o.doc[k] = v.value;
          }
        });
      });
  };
}
exports.validateDoc = validateDoc;

/**
 * Create a Highland stream transform function to turn objects into InfluxDB
 * line protocol records. Each input object should contain an object to
 * convert under the 'doc' property. This nested object must contain a time
 * property that's either a Javascript Date object or epoch milliseconds. This
 * time property is assumed and should not be described by the schema.
 * @param {string} measurement InfluxDB measurement name
 * @param {object} schema Object with key,value of property,type where the
 * properties dictate which parts of each input 'doc' object are included in
 * the Line Protocol output string, and their types (category, text, float,
 * integer,boolean). Category values become InfluxDB tags and all other values
 * become fields. Should not contain a time property.
 * @returns {Object} Highland stream transform function for use with through()
 */
function docToLineProtocol(measurement, schema, ensureSorted=true) {
  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new TimeSeriesCopError(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
  }
  schema = schemaValidation.schema;  // set validated, case-normalized schema
  let prevtime = null;  // track previous time to ensure ascending order

  return (stream) => {
    return stream.map(o => {
      const fields = {},
        tags = {};
      let time;

      Object.keys(o.doc).forEach(k => {
        // Only add properties which have defined values
        if (o.doc[k] !== null && o.doc[k] !== NaN && o.doc[k] !== undefined) {
          switch (schema[k]) {
            case 'text':
              const escapedText = prepareFieldText(o.doc[k]);
              if (escapedText) {
                fields[k] = escapedText;
              }
              break;
            case 'category':
              tags[k] = o.doc[k];
              break;
            case 'integer':
              fields[k] = o.doc[k] + 'i';
              break;
            case 'float':
              fields[k] = o.doc[k];
              break;
            case 'boolean':
              fields[k] = o.doc[k];
              break;
            case 'time':
              time = o.doc[k];  // moment, Date, or epoch ms
              break;
          }
        }
      });

      // If no fields present, mark missing data with 'influxMissingData' field
      if (Object.keys(fields).length === 0) {
        fields.influxMissingData = true;
      }

      // Timestamp must be present
      if (time === undefined) {
        throw new TimeSeriesCopError(`${validation.errorPrefix} time value missing from line ${o.lineIndex + 1}`);
      }
      if (ensureSorted && prevtime !== null && prevtime > +time) {
        throw new TimeSeriesCopError(`${validation.errorPrefix} records not in ascending chronological order near line ${o.lineIndex + 1}`);
      }
      prevtime = +time;

      return {
        measurement: measurement,
        ts: +time  * 1000 * 1000,  // nanoseconds since epoch
        tags: tags,
        fields: fields
      };
    })
    .through(new JsonInfluxDbStream())
    .map(lp => lp + '\n'); // add some newlines
  };
}
exports.docToLineProtocol = docToLineProtocol;

/**
 * Create a Highland stream transform function to turn each object into a point
 * ready for writing to InfluxDB. Each input object should contain an object to
 * convert under the 'doc' property. This nested object must contain a time
 * property that's either a Javascript Date object or epoch milliseconds. This
 * time property is assumed and should not be described by the schema.
 * @param {string} measurement InfluxDB measurement name
 * @param {object} schema Object with key,value of property,type where the
 * properties dictate which parts of each input 'doc' object are included in
 * the output point object, and their types (category, text, float,
 * integer,boolean). Category values become InfluxDB tags and all other values
 * become fields. Should not contain a time property.
 * @returns {Object} Highland stream transform function for use with through()
 */
function prepDocForInfluxDB(measurement, schema) {
  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new TimeSeriesCopError(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
  }
  schema = schemaValidation.schema;  // set validated, case-normalized schema

  return (stream) => {
    return stream
      .map(o => {
        const fields = {}, tags = {};
        let time;

        Object.keys(o.doc).forEach(k => {
          // Only add properties which have defined values
          if (o.doc[k] !== null && o.doc[k] !== NaN && o.doc[k] !== undefined) {
            switch (schema[k]) {
              case 'category':
                tags[k] = o.doc[k];
                break;
              case 'time':
                time = +o.doc[k];  // moment, Date, or epoch ms
                break;
              default:
                fields[k] = o.doc[k];
                break;
            }
          }
        });

        // If no fields present, mark missing data with 'influxMissingData' field
        if (_.keys(fields).length === 0) {
          fields.influxMissingData = true;
        }

        // Timestamp must be present
        if (time === undefined) {
          throw new TimeSeriesCopError(`${validation.errorPrefix} time value missing from line ${o.lineIndex + 1}`);
        }

        return { measurement, fields, tags, timestamp: time };
      });
  };
}
exports.prepDocForInfluxDB = prepDocForInfluxDB;

/**
 * Create a Highland stream transform function to turn batch and sort point
 * objects before writing them to an InfluxDB database. Each input object
 * should be an object ready to be passed to the Node InfluxDB driver's
 * writePoints method as a single record, with a timestamp.
 * @param {string} measurement InfluxDB measurement name
 * @param {object} schema Object with key,value of property,type where the
 * properties dictate which parts of each input 'doc' object are included in
 * the output point object, and their types (category, text, float,
 * integer,boolean). Category values become InfluxDB tags and all other values
 * become fields. Should not contain a time property.
 * @param {string} host InfluxDB host name
 * @param {string} database InfluxDB database name
 * @param {string} [batchSize=10000] How many points to write at a time
 * @returns {Object} Highland stream transform function for use with through()
 */
function writeDocsToInfluxDB({
  measurement=null,
  schema=null,
  host=null,
  database=null,
  batchSize=10000
} = {}) {
  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new TimeSeriesCopError(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
  }
  schema = schemaValidation.schema;  // set validated, case-normalized schema
  const influx = new Influx.InfluxDB({
    host,
    database,
    schema: [
      schema2InfluxSchema(schema, measurement)
    ]
  });

  return (stream) => {
    return stream
      .batch(batchSize)
      .map(points => points.sort((a, b) => a.timestamp - b.timestamp))
      .flatMap(points => H(influx.writePoints(points, { precision: 'ms' })));
  };
}
exports.writeDocsToInfluxDB = writeDocsToInfluxDB;

function schema2InfluxSchema(schema, measurement) {
  influxSchema = { measurement, tags: [], fields: {} };
  _.keys(schema).forEach(k => {
    switch (schema[k]) {
      case 'text':
        influxSchema.fields[k] = Influx.FieldType.STRING
        break;
      case 'category':
        influxSchema.tags.push(k);
        break;
      case 'integer':
        influxSchema.fields[k] = Influx.FieldType.INTEGER
        break;
      case 'float':
        influxSchema.fields[k] = Influx.FieldType.FLOAT
        break;
      case 'boolean':
        influxSchema.fields[k] = Influx.FieldType.BOOLEAN
        break;
    }
  });
  // If no fields present, mark missing data with 'influxMissingData' field
  influxSchema.fields.influxMissingData = Influx.FieldType.BOOLEAN;
  return influxSchema;
}

/**
 * Handle empty lines.
 * @param {boolean} [dropInternalBlank=true] Drop empty lines between non-empty
 * lines.
 * @param {boolean} [dropFinalBlank=true] Drop empty lines at the end of the
 * stream.
 */
function dropBlanks({dropInternalBlank=true, dropFinalBlank=true} = {}) {
  blankBuffer = [];
  return (err, x, push, next) => {
    if (err) {
      if (!dropInternalBlank) {
        // Push buffered internal empty lines
        blankBuffer.forEach(e => push(null, e));
      }
      blankBuffer = [];  // reset empty line buffer
      push(err);  // push current error
      next();
    } else if (x === H.nil) {
      // End of stream
      if (!dropFinalBlank) {
        // Push final empty lines, except for the entry always produced after
        // the last newline.
        blankBuffer.slice(0, blankBuffer.length-1).forEach(e => push(null, e));
      }
      blankBuffer = [];
      push(null, x);
    } else if (x.text.length === 0) {
      // Blank line
      blankBuffer.push(x);  // buffer another empty line
      next();
    } else {
      // Non-blank line
      if (!dropInternalBlank) {
        // Push buffered internal empty lines
        blankBuffer.forEach(e => push(null, e));
      }
      blankBuffer = [];  // reset empty line buffer
      push(null, x);  // push current line
      next();
    }
  }
}

// Split on whitespace.
// Won't handle quoted fields that contains whitespace.
// Leading and trailing whitespace is ignored.
function splitOnWhitespace(line) {
  return line.trim().split(/\s+/);
}

// Escape internal double-quotes. Double-quote if not already quoted.
function prepareFieldText(text) {
  if (!text) {
    return text;
  }
  if (text.indexOf('"') != -1) {  // contains "
    if (text[0] !== '"') {
      text = '"' + text.replace(/"/g, '\\"') + '"';  // escape "
    } // else already double-quoted so do nothing
  } else {
    // No double-quotes in text so quote it
    text = '"' + text + '"';
  }
  return text;
}
