#!/usr/bin/env node

const fs = require('fs')
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

const argv = tscop.standardCli();

const inputStream = fs.createReadStream(argv.input, {encoding: 'utf8'});
const outputStream = fs.createWriteStream(argv.output);
tscop.parseStandardFile(inputStream, outputStream)
  .then(result => console.log(result))
  .catch(err => {
    if (err instanceof TimeSeriesCopError) {
      console.log(err.message);
    } else {
      throw err;
    }
  });
