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

/**
 * Return the distance between two coordinates in km
 * http://stackoverflow.com/questions/365826/calculate-distance-between-2-gps-coordinates
 * by cletus.  Which answer was itself based on http://www.movable-type.co.uk/scripts/latlong.html
 * @param lonlat1 two-item array of decimal degree longitude and latitude for point 1
 * @param lonlat2 two-item array of decimal degree longitude and latitude for point 2
 * @returns km from 1 to 2
 */
function geo2km(lonlat1, lonlat2) {
  if (! lonlat1 || ! lonlat2) {
    return null;
  }
  if (!(_.isFinite(lonlat1[0]) && _.isFinite(lonlat1[1]) &&
      _.isFinite(lonlat2[0]) && _.isFinite(lonlat2[1]))) {
    return null;
  }

  const toRad = function(degree) { return degree * (Math.PI / 180); };
  const R = 6371; // km radius of Earth
  const dLat = toRad(lonlat2[1] - lonlat1[1]);
  const dLon = toRad(lonlat2[0] - lonlat1[0]);
  const lat1 = toRad(lonlat1[1]);
  const lat2 = toRad(lonlat2[1]);

  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
          Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  const d = R * c;
  return d;
}
exports.geo2km = geo2km;

/**
 * Return speed in km/h traveling between lonlat1 and lonlat2 during time
 * interval t1 to t2.
 * @param lonlat1 two-item array of decimal degree longitude and latitude for point 1
 * @param lonlat2 two-item array of decimal degree longitude and latitude for point 2
 * @param t1 ms since epoch for point 1
 * @param t2 ms since epoch for point 2
 * @returns speed in km/h traveling between 1 and 2
 */
function geo2kmph(lonlat1, lonlat2, t1, t2) {
  const km = geo2km(lonlat1, lonlat2);
  const hours= (t2 - t1) / 1000 / 60 / 60;
  if (km === null) {
    return null;
  }
  return km / hours;
}
exports.geo2kmph = geo2kmph;
