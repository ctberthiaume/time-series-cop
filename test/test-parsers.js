const chai = require('chai');
const expect = chai.expect;
const validation = require('../src/lib/validation');
const parser = require('../src/lib/standardParser');
const stream = require('stream');

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
        expect(result).to.equal('Success. Wrote 1 points.');
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
        expect(result).to.equal('Success. Wrote 1 points.');
    });
  });
});
