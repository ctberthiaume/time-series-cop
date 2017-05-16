const fs = require('fs');
const H = require('highland');
const CSV = require('csv-string');
const _ = require('lodash');
const eolFix = require('eol-fix-stream');
const JsonInfluxDbStream = require('json-to-influxdb-line').JsonInfluxDbStream;
const validation = require('./validation');

// ***************************************************************************
// Exported
// ***************************************************************************

/**
 * Process command-line arguments for a text file to Line Protocol script.
 * @returns {Object} yargs argv object
 */
function cli() {
  const argv = require('yargs')
    .usage('Usage: $0 [options]')
    .example('$0 -c KOK1606 -m seaflow -f seaflow.csv')
    .describe('c', 'Cruise name for Line Protocol tags')
    .alias('c', 'cruise')
    .nargs('c', 1)
    .describe('m', 'InfluxDB measurement name')
    .alias('m', 'measurement')
    .nargs('m', 1)
    .describe('i', 'Input file path. Will be converted to a node readable stream.')
    .alias('i', 'input')
    .nargs('i', 1)
    .describe('t', 'Create a tailing stream of input file contents (always-tail npm module)')
    .alias('t', 'tail')
    .boolean('t')
    .describe('o', 'Output file path. Will be converted to a node writable stream. - for stdout.')
    .alias('o', 'output')
    .nargs('o', 1)
    .default('o', '-')
    .demandOption(['i'])
    .argv;
  return argv;
}
exports.cli = cli;

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
      fields = splitOnWhitespace(o.text)
      // Empty lines become single item arrays of empty string
      // Turn into empty array
      o.fields = fields.length && fields[0] === '' ? [] : fields;
    });
  } else {
    pipe = pipe.doto(o => {
      fields = CSV.parse(o.text, delimiter)[0];
      // Empty lines become single item arrays of empty string
      // Turn into empty array
      o.fields = fields.length && fields[0] === '' ? [] : fields;
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
          throw new Error(`${validation.errorPrefix} Column count (${o.fields.length}) does not match header count (${headers.length}) on line ${o.lineIndex + 1}`);
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
    throw new Error(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
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
              throw new Error(`Invalid type '${schema[k]}'`);
            }
            const v = validator[schema[k]](o.origDoc[k]);
            if (v.error) {
              throw new Error(`${validation.errorPrefix} ${v.error} on line ${o.lineIndex + 1}. column=${k}, value=${o.origDoc[k]}, type=${schema[k]}`);
            }
            o.doc[k] = v.value;
          }
        });
      });
  };
}
exports.validateDoc = validateDoc;
exports.validateTypeArray = validation.validateTypeArray;
exports.errorPrefix = validation.errorPrefix;

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
function docToLineProtocol(measurement, schema) {
  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new Error(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
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
        throw new Error(`${validation.errorPrefix} time value missing from line ${o.lineIndex + 1}`);
      }
      if (prevtime !== null && prevtime > +time) {
        throw new Error(`${validation.errorPrefix} records not in ascending chronological order near line ${o.lineIndex + 1}`);
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
 * Convert a GGA latitude or longitude coordinate to decimal degrees.
 * @param gga GGA coordinate string. e.g. 4023.0117S or -4023.0117
 * @returns Decimal degree coordinate string e.g. -40.3835
 */
function GGAToDecimalDegrees(gga) {
  if (! gga || ! gga.length) {
    return null;
  }

  let sign = 1,
    dotidx, degrees, minutes, nsew, decimalDegrees;

  // Figure out the sign of the coordinate
  const last = gga[gga.length-1].toUpperCase();
  if (last === 'E' || last === 'S') {
    sign = -1;
    gga = gga.slice(0, gga.length - 1);
  } else if (last === 'W' || last === 'N') {
    gga = gga.slice(0, gga.length - 1);
  } else if (gga[0] === '-') {
    sign = -1;
    gga = gga.slice(1);  // remove sign char for now
  }

  dotidx = gga.indexOf('.');

  if (dotidx !== -1) {
    degrees = gga.slice(0, dotidx - 2);
    minutes = gga.slice(dotidx - 2, gga.length)
  } else {
    // In case the decimal part of the minutes is left off?
    degrees = gga.slice(0, -2);
    minutes = gga.slice(-2);
  }

  decimalDegrees = sign * (+degrees + (minutes / 60));

  return _.round(decimalDegrees, 4).toString();
}
exports.GGAToDecimalDegrees = GGAToDecimalDegrees;


// ***************************************************************************
// Not exported
// ***************************************************************************

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
