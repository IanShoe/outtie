const csvWriter = require('csv-write-stream');
const fs = require('fs');
const transform = require('stream-transform');

const writerMap = {};

module.exports = function (splits) {
  if (!Array.isArray(splits)) {
    throw new Error('Splits must be an Array');
  }
  splits.forEach(function (split) {
    if (!split.filePath) {
      throw new Error('Split must have File Path Property');
    }
    if (!split.name) {
      throw new Error('Split must have Name Property');
    }
    if (!split.headers) {
      throw new Error('Split must have Header Property');
    }
    const writer = csvWriter({
      headers: split.headers
    });
    writer.pipe(fs.createWriteStream(split.filePath));
    writerMap[split.name] = writer;
  });
  return {
    write: transform(function (record, cb) {
      for (let key in record) {
        if (record.hasOwnProperty(key)) {
          const writer = writerMap[key];
          if (record[key] !== undefined && record[key] !== null) {
            if (Array.isArray(record[key])) {
              record[key].forEach(writer.write.bind(writer));
            } else {
              writer.write(record[key]);
            }
          }
        }
      }
      cb();
    }),
    end: function (err) {
      for (let key in writerMap) {
        if (writerMap.hasOwnProperty(key)) {
          writerMap[key].end();
        }
      }
    }
  };
};
