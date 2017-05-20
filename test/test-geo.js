const chai = require('chai');
const should = chai.should();
const tscop = require('../src/lib/index');

describe('Geo', () => {
  it('should convert GGA to decimal degrees', done => {
    const results = {
      ddnorth1: tscop.GGAToDecimalDegrees('0124.5177N'),
      ddnorth2: tscop.GGAToDecimalDegrees('1224.5177N'),
      ddnorth3: tscop.GGAToDecimalDegrees('0124.5177'),
      ddnorth4: tscop.GGAToDecimalDegrees('1224.5177'),
      ddsouth1: tscop.GGAToDecimalDegrees('0124.5177S'),
      ddsouth2: tscop.GGAToDecimalDegrees('1224.5177S'),
      ddsouth3: tscop.GGAToDecimalDegrees('-0124.5177'),
      ddsouth4: tscop.GGAToDecimalDegrees('-1224.5177'),
      ddeast1: tscop.GGAToDecimalDegrees('0124.5177E'),
      ddeast2: tscop.GGAToDecimalDegrees('1224.5177E'),
      ddeast3: tscop.GGAToDecimalDegrees('12224.5177E'),
      ddeast4: tscop.GGAToDecimalDegrees('0124.5177'),
      ddeast5: tscop.GGAToDecimalDegrees('1224.5177'),
      ddeast6: tscop.GGAToDecimalDegrees('12224.5177'),
      ddwest1: tscop.GGAToDecimalDegrees('0124.5177W'),
      ddwest2: tscop.GGAToDecimalDegrees('1224.5177W'),
      ddwest3: tscop.GGAToDecimalDegrees('12224.5177W'),
      ddwest4: tscop.GGAToDecimalDegrees('-0124.5177'),
      ddwest5: tscop.GGAToDecimalDegrees('-1224.5177'),
      ddwest6: tscop.GGAToDecimalDegrees('-12224.5177'),
      ddnodecimal: tscop.GGAToDecimalDegrees('12224')
    };
    const output = {
      ddnorth1: '1.4086',
      ddnorth2: '12.4086',
      ddnorth3: '1.4086',
      ddnorth4: '12.4086',
      ddsouth1: '-1.4086',
      ddsouth2: '-12.4086',
      ddsouth3: '-1.4086',
      ddsouth4: '-12.4086',
      ddeast1: '1.4086',
      ddeast2: '12.4086',
      ddeast3: '122.4086',
      ddeast4: '1.4086',
      ddeast5: '12.4086',
      ddeast6: '122.4086',
      ddwest1: '-1.4086',
      ddwest2: '-12.4086',
      ddwest3: '-122.4086',
      ddwest4: '-1.4086',
      ddwest5: '-12.4086',
      ddwest6: '-122.4086',
      ddnodecimal: '122.4'
    }

    results.should.deep.equal(output);
    done();
  });
});
