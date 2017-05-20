const _ = require('lodash');

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
  if (last === 'W' || last === 'S') {
    sign = -1;
    gga = gga.slice(0, gga.length - 1);
  } else if (last === 'E' || last === 'N') {
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
