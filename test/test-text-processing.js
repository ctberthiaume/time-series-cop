const chai = require('chai');
const should = chai.should();
const tscop = require('../src/lib/index');

describe('Lines', () => {
  it('should ignore internal blank lines', done => {
    tscop
      .lineStream({
        stream: ['line1\n', '\n', 'line3\n'],
        dropInternalBlank: true
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line3', lineIndex: 2 });
        done();
      });
  });
  it('should keep internal blank lines', done => {
    tscop
      .lineStream({
        stream: ['line1\n', '\n', 'line3\n'],
        dropInternalBlank: false
      })
      .toArray(x => {
        x.should.have.length(3);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: '', lineIndex: 1 });
        x[2].should.deep.equal({ text: 'line3', lineIndex: 2 });
        done();
      });
  });
  it('should ignore final blank lines', done => {
    tscop
      .lineStream({
        stream: ['line1\n', 'line2\n', '\n'],
        dropFinalBlank: true
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line2', lineIndex: 1 });
        done();
      });
  });
  it('should keep final blank lines', done => {
    tscop
      .lineStream({
        stream: ['line1\n', 'line2\n', '\n'],
        dropFinalBlank: false
      })
      .toArray(x => {
        x.should.have.length(3);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line2', lineIndex: 1 });
        x[2].should.deep.equal({ text: '', lineIndex: 2 });
        done();
      });
  });
  it('should handle CR, LF, or CRLF line endings', done => {
    tscop
      .lineStream({
        stream: ['line1\r', 'line2\n', 'line3\r\n', 'line4\n']
      })
      .toArray(x => {
        x.should.have.length(4);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line2', lineIndex: 1 });
        x[2].should.deep.equal({ text: 'line3', lineIndex: 2 });
        x[3].should.deep.equal({ text: 'line4', lineIndex: 3 });
        done();
      });
  });
  it('should handle case where final line does not end in newline', done => {
    tscop
      .lineStream({
        stream: ['line1\n', 'line2'],
        dropFinalBlank: false
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line2', lineIndex: 1 });
        done();
      });
  });
  it('should start at second line', done => {
    tscop
      .lineStream({
        stream: ['line1\n', 'line2\n', 'line3\n'],
        start: 1
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.deep.equal({ text: 'line2', lineIndex: 1 });
        x[1].should.deep.equal({ text: 'line3', lineIndex: 2 });
        done();
      });
  });
  it('should end with second line', done => {
    tscop
      .lineStream({
        stream: ['line1\n', 'line2\n', 'line3\n'],
        end: 2
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.deep.equal({ text: 'line1', lineIndex: 0 });
        x[1].should.deep.equal({ text: 'line2', lineIndex: 1 });
        done();
      });
  });
  it('should keep lines when only skipping internal blank lines and all lines are empty', done => {
    tscop
      .lineStream({
        stream: ['\n', '\n'],
        dropInternalBlank: true,
        dropFinalBlank: false
      })
      .toArray(x => {
        x.should.have.length(2);
        done();
      });
  });
});

describe('Fields', () => {
  it('split on whitespace', done => {
    tscop
      .fieldStream({
        stream: ['a b    \tc\n'],
        delimiter: 'whitespace'
      })
      .toArray(x => {
        x[0].should.deep.equal({
          text: 'a b    \tc',
          lineIndex: 0,
          recordIndex: 0,
          fields: ['a', 'b', 'c']
        });
        done();
      });
  });
  it('split on arbitrary string', done => {
    tscop
      .fieldStream({
        stream: ['a\t b\tc\n'],
        delimiter: '\t'
      })
      .toArray(x => {
        x[0].should.deep.equal({
          text: 'a\t b\tc',
          lineIndex: 0,
          recordIndex: 0,
          fields: ['a', ' b', 'c']
        });
        done();
      });
  });
  it('create empty field array for blank line', done => {
    tscop
      .fieldStream({
        stream: ['\n'],
        delimiter: '\t',
        dropInternalBlank: false,
        dropFinalBlank: false
      })
      .toArray(x => {
        x[0].fields.should.have.length(0);
        done();
      });
  });
  it('skip recordIndex increment when skipping lines', done => {
    tscop
      .fieldStream({
        stream: ['line1\n', '\n', 'line3\n'],
        dropInternalBlank: true
      })
      .toArray(x => {
        x.should.have.length(2);
        x[0].should.have.property('lineIndex', 0);
        x[0].should.have.property('recordIndex', 0);
        x[1].should.have.property('lineIndex', 2);
        x[1].should.have.property('recordIndex', 1);
        done();
      });
  });
});
