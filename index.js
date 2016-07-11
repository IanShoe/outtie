const csvWriter = require('csv-write-stream');
const fs = require('fs');
const transform = require('stream-transform');

const writerMap = {};

module.exports = function (splits) {
  splits.forEach(function (split) {
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
      if (err) {
        console.log('error', err);
      } else {
        for (let key in writerMap) {
          if (writerMap.hasOwnProperty(key)) {
            writerMap[key].end();
          }
        }
        process.exit();
      }
    }
  };
};
