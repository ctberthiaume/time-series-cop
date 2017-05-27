const chai = require('chai');
const should = chai.should();
const expect = chai.expect;
const validation = require('../src/lib/validation');

describe('Schema', () => {
  it('should normalize types to lowercase', done => {
    const schema = {
      'prop1': 'TeXt',
      'prop2': 'text'
    };
    const validated = validation.validateSchema(schema);
    validated.should.have.property('error', null);
    validated.should.have.deep.property('schema.prop1', 'text');
    validated.should.have.deep.property('schema.prop2', 'text');
    done();
  });
  it('should accept valid schema', done => {
    const schema = {
      'prop1': 'text',
      'prop2': 'time',
      'prop3': 'category',
      'prop4': 'integer',
      'prop5': 'float',
      'prop6': 'boolean'
    };
    const validated = validation.validateSchema(schema);
    validated.should.have.property('error', null);
    validated.should.have.property('schema').that.deep.equals(schema);
    done();
  });
  it('should reject invalid schema', done => {
    const schema = {
      'prop1': 'text',
      'prop2': 'badType',
      'prop3': 'category',
      'prop4': 'integer',
      'prop5': 'float',
      'prop6': 'boolean'
    };
    const validated = validation.validateSchema(schema);
    validated.should.have.property('error', 'badType');
    validated.should.have.property('schema').that.equals(schema);
    done();
  });
  it('should reject invalid measurement name', done => {
    expect(validation.validateMeasurement(';drop measurement data;')).to.be.null;
    done();
  });
  it('should reject blank measurement name', done => {
    expect(validation.validateMeasurement('')).to.be.null;
    done();
  });
  it('should reject measurement name of only spaces', done => {
    expect(validation.validateMeasurement('  ')).to.be.null;
    done();
  });
  it('should accept valid measurement name', done => {
    expect(validation.validateMeasurement('A-b_C')).to.not.be.null;
    done();
  });
});
