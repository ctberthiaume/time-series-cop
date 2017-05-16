const validator = require('validator');
const moment = require('moment');
const _ = require('lodash');

const validTypes = ['category', 'text', 'float', 'integer', 'boolean', 'time'];
const errorPrefix = 'ValidationError:';

function validated(msg, val) {
  return { error: msg, value: val };
}

// Validate data. 'NA' for any type means missing data and should be set to
// null. All validators return two an object:
// {
//   error: {string} or null,
//   value: original value or null if 'NA'
// }
const strict = {
  text(value) {
    if (value === 'NA') return { error: null, value: null };
    if (value === '') return { error: 'Empty text value', value };
    return { error: null, value };
  },
  category(value) {
    if (value === 'NA') return { error: null, value: null };
    if (value === '') return { error: 'Empty category value', value };
    return { error: null, value };
  },
  float(value) {
    if (validator.isFloat(value)) return { error: null, value };
    if (value === 'NA') return { error: null, value: null };
    if (value === '') return { error: 'Empty float value', value };
    return { error: 'Not a float', value };
  },
  integer(value) {
    if (validator.isInt(value)) return { error: null, value };
    if (value === 'NA') return { error: null, value: null };
    if (value === '') return { error: 'Empty integer value', value };
    return { error: 'Not an integer', value };
  },
  boolean(value) {
    if (value === 'TRUE' || value === 'FALSE') return { error: null, value };
    if (value === 'NA') return { error: null, value: null };
    if (value === '') return { error: 'Empty boolean value', value };
    return { error: 'Invalid boolean value', value };
  },
  time(value) {
    const m = moment.utc(value, moment.ISO_8601, true);
    if (!m.isValid()) return { error: 'Invalid ISO8601 timestamp', value };
    return { error: null, value: m };
  }
}

// If value fails validation, set to null. Otherwise return the original value,
// or in the case of boolean 'TRUE' or 'FALSE'
const lax = {
  text(value) {
    if (value === '') value = null;
    return { error: null, value };
  },
  category(value) {
    if (value === '') value = null;
    return { error: null, value };
  },
  float(value) {
    if (!validator.isFloat(value)) value = null;
    return { error: null, value };
  },
  integer(value) {
    if (!validator.isInt(value)) value = null;
    return { error: null, value };
  },
  boolean(value) {
    value = validator.toBoolean(val.toLowerCase()) ? 'TRUE' : 'FALSE';
    return { error: null, value };
  },
  time(value) {
    const m = moment.utc(value, moment.ISO_8601, true);
    if (!m.isValid()) return { error: 'Invalid ISO8601 timestamp', value };
    return { error: null, value: m };
  }
}

// Validate a schema object
// Return { error, schema }, where is null for good validation, or  the
// offending type for bad validation. schema is a clone of original schema with
// type case normalized to lower-case.
function validateSchema(schema) {
  const vschema = _.clone(schema);  // validated and type text case-normalized schema

  const toCheck = _.values(vschema);
  for (let i=0; i<toCheck.length; i++) {
    if (!_.includes(validTypes, toCheck[i].toLowerCase())) {
      return { error: toCheck[i], schema: schema };
    }
  }
  // Normalize case, after possibly returning error so that error type
  // matches user input.
  _.keys(vschema).forEach(k => vschema[k] = vschema[k].toLowerCase());
  return { error: null, schema: vschema };
}

exports.validators = { strict, lax };
exports.validateSchema = validateSchema;
exports.validTypes = validTypes;
exports.errorPrefix = errorPrefix;
