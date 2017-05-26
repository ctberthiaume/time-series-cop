const chai = require('chai');
const expect = chai.expect;
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const validation = require('../src/lib/validation');
const parser = require('../src/lib/standardParser');
const stream = require('stream');
const TimeSeriesCopError = require('../src/lib/error').TimeSeriesCopError;

let outArray = [];
let output;

beforeEach(function() {
  outArray = [];
  output = new stream.Writable({
  	write: function(chunk, encoding, next) {
    	outArray.push(chunk.toString());
    	next();
  	}
	});
});

describe('Standard Format', function() {
  it('should produce one line of line protocol', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2,des3,desc4,desc5,desc6\n',
      'time,float,integer,text,category,boolean\n',
      'NA,m/s,km,NA,NA,NA\n',
      'time,speed,distance,notes,group,flag\n',
      '2017-05-06T19:52:57.601Z,6.0,10,some notes,A,TRUE\n'
    ];
    return parser.getStandardHeader(input, ',')
      .then(header => parser.validateStandardHeader(header))
      .then(header => parser.parseStandardBody(input, output, header, ','))
      .then(result => {
        expect(outArray).to.have.length(1);
        expect(outArray).to.have.deep.property(
          '[0]',
          'fileType,group=A,cruise=cruise speed=6.0,distance=10i,notes="some notes",flag=TRUE 1494100377601000000\n'
        );
        expect(result).to.equal('Success. fileType. Wrote 1 points.');
      });
  });
  it('should produce one "missing data" line of line protocol', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return parser.getStandardHeader(input, ',')
      .then(header => parser.validateStandardHeader(header))
      .then(header => parser.parseStandardBody(input, output, header, ','))
      .then(result => {
        expect(outArray).to.have.length(1);
        expect(outArray).to.have.deep.property(
          '[0]',
          'fileType,cruise=cruise influxMissingData=true 1494100377601000000\n'
        );
        expect(result).to.equal('Success. fileType. Wrote 1 points.');
    });
  });
  it('should reject if file type is missing', function() {
    const input = [
      '\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if cruise is missing', function() {
    const input = [
      'fileType\n',
      '\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if description is missing', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      '\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if column descriptions are missing', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      '\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank column description', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if bad type', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,floatnotrealtype\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank type', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank type line', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      '\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if time is not first type', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'float,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank unit', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      ',m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank unit line', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      '\n',
      'time,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if time is not first header column', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'nottime,speed\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank header column', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank header line', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      '\n',
      '2017-05-06T19:52:57.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if bad timestamp', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      'time,speed\n',
      '2017-05-06T19:52:5aaa7.601Z,NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if blank data column', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      '\n',
      '2017-05-06T19:52:57.601Z,\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
  it('should reject if empty time data column', function() {
    const input = [
      'fileType\n',
      'cruise\n',
      'description\n',
      'desc1,desc2\n',
      'time,float\n',
      'NA,m/s\n',
      '\n',
      ',NA\n'
    ];
    return expect(
      parser.getStandardHeader(input, ',')
        .then(header => parser.validateStandardHeader(header))
        .then(header => parser.parseStandardBody(input, output, header, ','))
    ).to.eventually.be.rejectedWith(TimeSeriesCopError);
  });
});
