function TimeSeriesCopError(message, data) {
  this.name = 'TimeSeriesCopError';
  this.message = message || 'Unknown Time Series Cop error';
  this.stack = (new Error()).stack;
  this.data = data;
}
TimeSeriesCopError.prototype = Object.create(Error.prototype);
TimeSeriesCopError.prototype.constructor = TimeSeriesCopError;
exports.TimeSeriesCopError = TimeSeriesCopError;
