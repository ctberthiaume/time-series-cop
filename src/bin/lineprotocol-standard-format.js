#!/usr/bin/env node

const fs = require('fs')
  tscop = require('../lib/index'),
  TimeSeriesCopError = require('../lib/error').TimeSeriesCopError;

const argv = tscop.standardCli();

const inputStream = fs.createReadStream(argv.input, {encoding: 'utf8'});

let p;
if (argv.host && argv.db) {
  p = tscop.parseStandardFileToDB(inputStream, argv.host, argv.db);
} else {
  p = tscop.parseStandardFile(inputStream, fs.createWriteStream(argv.output));
}
p.then(result => console.log(result))
  .catch(e => {
    if (e instanceof TimeSeriesCopError) {
      console.log(`${e.name}: ${e.message}`);
    } else {
      console.log(e.stack);
    }
    process.exit(1);
  });
