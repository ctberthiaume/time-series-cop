const fs = require('fs');
const H = require('highland');
const moment = require('moment');
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
 * @param {Object} [instream=null] Input to Highland stream constructor. May be
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
  instream=null,
  start=0,
  end=Infinity,
  dropInternalBlank=true,
  dropFinalBlank=true
} = {}) {
  let i = 0;
  return H(instream)  // make a highland stream
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
 * @param {Object} [instream=null] Input to Highland stream constructor. May be
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
  instream=null,
  start=0,
  end=Infinity,
  dropInternalBlank=true,
  dropFinalBlank=true,
  delimiter='\t'
} = {}) {
  let i = 0;
  let pipe = lineStream({instream, start, end, dropInternalBlank, dropFinalBlank});
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
 * @param {boolean} If true, thow TimeSeriesCopError if
 * fields length !== headers length.
 * @returns {Object} Highland stream transform function for use with through()
 */
function fieldsToDoc(headers, strict) {
  return (stream) => {
    if (!strict) {
      stream = stream.filter(o => o.fields.length === headers.length);
    }
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
            if (o.origDoc[k] !== null) {
              const v = validator[schema[k]](o.origDoc[k].trim());
              if (v.error) {
                throw new TimeSeriesCopError(`${v.error} on line ${o.lineIndex + 1}. column=${k}, value=${o.origDoc[k]}, type=${schema[k]}`);
              }
              o.doc[k] = v.value;
            }
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
 * @param {boolean} [ensureSorted=true] Throw an error if points are not in
 * ascending chronological order.
 * @param {Object} outstream Node writable stream that line protocol lines will
 * be written to.
 * @returns {Object} Highland stream transform function for use with through()
 */
function writeDocToLineProtocol({
  measurement=null,
  schema=null,
  ensureSorted=true,
  outstream=null
} = {}) {
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
              if (o.doc[k]) {
                fields[k] = Influx.escape.quoted(o.doc[k]);
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
    .doto(x => outstream.write(x + '\n'));
  };
}
exports.writeDocToLineProtocol = writeDocToLineProtocol;

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
function prepDocForInfluxDB({measurement=null, schema=null} = {}) {
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
              case undefined:
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
 * @param {number} [batchSize=10000] How many points to write at a time
 * @param {number} [windowSize=3] InfluxDB GROUP BY time() window size in minutes
 * @returns {Object} Highland stream transform function for use with through()
 */
function writeDocToInfluxDB({
  measurement=null,
  schema=null,
  host=null,
  database=null,
  batchSize=10000,
  windowSize=3  // in minutes
} = {}) {
  windowSize = parseInt(windowSize);
  // Validate schema types
  const schemaValidation = validation.validateSchema(schema);
  if (schemaValidation.error) {
    throw new TimeSeriesCopError(`${validation.errorPrefix} Invalid type '${schemaValidation.error}'`);
  }
  schema = schemaValidation.schema;  // set validated, case-normalized schema
  const influxSchema = schema2InfluxSchema(schema, measurement);

  // To keep tags after downsampling query we have to explicitly name them in
  // the GROUP BY after time()
  let taglist;
  if (influxSchema.tags.length) {
    taglist = ',' + influxSchema.tags.map(t => `"${t}"`).join(',');
  }
  // Now to get around the fact that influxdb will rename fields to
  // mean_originalname if we use a * wildcard as field selector, we
  // need to explicitly construct a selector for all numeric fields
  // with AS to keep the original name.
  const fieldSelector = _.keys(influxSchema.fields).filter(f => {
    return (influxSchema.fields[f] === Influx.FieldType.INTEGER || influxSchema.fields[f] === Influx.FieldType.FLOAT);
  }).map(f => `MEAN("${f}") AS "${f}"`).join(',');

  const influx = new Influx.InfluxDB({
    host,
    database,
    schema: [ influxSchema ]
  });

  return (stream) => {
    return stream
      .through(prepDocForInfluxDB({measurement, schema}))
      .batch(parseInt(batchSize))
      .map(points => points.sort((a, b) => a.timestamp - b.timestamp))
      .flatMap(points => {
        if (windowSize) {
          // Downsample to time resolution of windowSize minutes
          // First get the time range
          const first = moment.utc(_.first(points).timestamp);
          const last = moment.utc(_.last(points).timestamp);
          // Move first point back by 1 window
          first.subtract(windowSize, 'minutes');

          return H(
            influx.ping(5000).then(hosts => {
              return new Promise((resolve, reject) => {
                setImmediate(() => {
                  hosts.forEach(host => {
                    if (!host.online) {
                      reject(new Error('Could not connect to database ' + host.url.host));
                    }
                  });
                  resolve();
                });
              });
            }).then(() => {
              return influx.writePoints(points, { precision: 'ms' });
            }).then(() => {
              const query = `
                SELECT ${fieldSelector}
                INTO "viz"."autogen"."${measurement}"
                FROM "${measurement}"
                WHERE time >= '${first.toISOString()}' AND time <= '${last.toISOString()}'
                GROUP BY time(${windowSize}m)${taglist};
              `;
              // console.log(query);
              return influx.query(query);
            })
          );
        } else {
          // Don't run downsampling query
          return H(
            influx.ping(5000).then(hosts => {
              return new Promise((resolve, reject) => {
                setImmediate(() => {
                  hosts.forEach(host => {
                    if (!host.online) {
                      reject(new Error('Could not connect to database ' + host.url.host));
                    }
                  });
                  resolve();
                });
              });
            }).then(() => {
              return influx.writePoints(points, { precision: 'ms' });
            })
          );
        }
      });
  };
}
exports.writeDocToInfluxDB = writeDocToInfluxDB;

function saveData({
  measurement=null,
  schema=null,
  host=null,
  database=null,
  outstream=null,
  batchSize=10000,
  windowSize=3
} = {}) {
  let count = 0;
  let error;

  return (stream) => {
    // Add a counter
    stream = stream.doto(o => count++);

    // Output data to InfluxDB or line protocol file
    if (database && host) {
      stream = stream.through(writeDocToInfluxDB({
        measurement,
        schema,
        host,
        database,
        batchSize,
        windowSize
      }));
    } else {
      stream = stream.through(writeDocToLineProtocol({
        measurement,
        schema,
        outstream
      }));
    }

    // Catch errors in pipeline and print or throw.
    // This ends the pipeline
    stream = stream.stopOnError(e => {
      error = e;
      if (e instanceof TimeSeriesCopError) {
        console.log(`${e.name}: ${e.message}`);
        // Not ideal to exit here, but makes writing scripts easier
        process.exit(1);
      } else {
        throw e;
      }
    });

    // This always runs, even if we stop for errors
    // This also starts stream consumption
    stream = stream.done(() => {
      if (!error) console.log('Success. Wrote ' + count + ' points.');
    });

    return stream;
  };
}
exports.saveData = saveData;

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
  influxSchema.tags.sort();
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
