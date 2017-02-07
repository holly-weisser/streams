'use strict';

const _ = require('lodash');
const stream = require('stream');
const through2 = require('through2');
const P = require('bluebird');
const Transform = require('stream').Transform;
const JsonStream = require('./json-stream').JsonStream;
const XmlStream = require('./xml-stream').XmlStream;
const csvStream = require('csv-write-stream');

exports.readArray = function(array) {
    let index = 0;
    const s = new stream.Readable({ objectMode: true });
    array = _.isArray(array) ? array : [].slice.call(arguments);
    s._read = function() {
        if (index < array.length) {
            this.push(array[index++]);
        } else {
            this.push(null);
        }
    };

    return s;
};


exports.writeArray = function(done) {
    const res = [];
    const s = new stream.Writable({ objectMode: true });
    s._write = function(record, enc, done) {
        res.push(record);
        done();
    };

    s.on('finish', function () {
        done(null, res);
    });

    s.on('error', done);

    return s;
};

exports.streamToArray = function streamToArray(stream) {
    return new P(function (resolve, reject) {
        stream.pipe(exports.writeArray(function (err, records) {
            if (err) return reject(err);

            resolve(records);
        }));
    });
};

exports.streamify = function (value) {
    if (value instanceof stream.Stream) return value;

    return exports.readArray(_.isArray(value) ? value : Array.prototype.slice.call(arguments));
};

exports.writeBuffer = function(inStream) {
    return new P(function (resolve, reject) {
        const writeStream = new stream.Writable();
        const buffers = [];
        writeStream._write = function(chunk, enc, next) {
            buffers.push(chunk);
            next();
        };
        inStream.pipe(writeStream)
            .on('finish', function () {
                resolve(Buffer.concat(buffers));
            })
            .on('error', reject);
    });
};

exports.mapSync = function(mapFn, flushFn) {
    return new stream.Transform({
        objectMode: true,
        transform: function (record, enc, done) {
            try {
                done(null, mapFn(record));
            } catch (err) {
                done(err);
            }
        },
        flush: flushFn
    });
};

exports.map = function(mapFn, flushFn) {
    return new stream.Transform({
        objectMode: true,
        transform: function (record, enc, done) {
            try {
                mapFn(record, done);
            } catch (err) {
                done(err);
            }
        },
        flush: flushFn
    });
};

exports.eachSync = function(eachFn, flushFn) {
    return new stream.Transform({
        objectMode: true,
        transform: function(record, enc, done) {
            try {
                eachFn(record);
                done(null, record);
            } catch (err) {
                done(err);
            }
        },
        flush: flushFn
    })
};

exports.each = function(eachFn, flushFn) {
    return new stream.Transform({
        objectMode: true,
        transform: function (record, enc, done) {
            try {
                eachFn(record, function (err) {
                    done(err, record);
                });
            } catch (err) {
                done(err);
            }
        },
        flush: flushFn
    });
};

exports.writeObject = function (done) {
    let result;

    return through2.obj(function (record, enc, done) {
        result = record;
        done();
    })
        .once('finish', function () {
            done(null, result);
        })
        .once('error', done);
};

exports.streamPromise = function(writeStream) {
    return new P(function (resolve, reject) {
        writeStream
            .on('finish', resolve)
            .on('end', reject);
    });
};

exports.drainStream = function(options) {
    const writeStream = new stream.Writable(options);
    writeStream._write = function(record, enc, done) {
        done();
    };

    return writeStream;
};

exports.pipeline = function(streams) {
    return exports.pipelineBuilder(...arguments).build();
};

exports.filter = function(predicate) {
    return new exports.FilterStream(predicate);
};

exports.flush = function(client){
    return new exports.FlushStream(client)
};

exports.omit = function(fields) {
    return exports.mapSync(r => _.omit(r, fields));
};

exports.pipelineBuilder = function(stream) {
    const streams = [].slice.call(arguments);
    return {
        pipe: function(s) {
            streams.push(s);
            return this;
        },
        build: function() {
            return streams.reduce(function (prev, next) {
                prev.on('error', err => next.emit('error', err));
                return prev.pipe(next);
            });
        }
    };
};

exports.peek = function(callback) {
    return new exports.PeekStream(callback);
};

exports.batch = function(batchSize) {
    return new exports.BatchStream(batchSize);
};

exports.buffer = function(bufferSize, opts) {
    return new exports.BufferStream(bufferSize, opts);
};

exports.json = function(opts) {
    return new JsonStream(opts);
};

exports.csv = function(opts) {
    return csvStream(opts);
};

exports.xml = function(opts) {
    return new XmlStream(opts);
};

exports.stringify = function(opts) {
    opts = _.assign({}, opts, {
        readableObjectMode: false,
        writeableObjectMode: true,
        transform: (r, e, d) => d(null, r)
    });

    return new Transform(opts);
};

exports.BatchStream = require('./batch-stream').BatchStream;
exports.BufferStream = require('./buffer-stream').BufferStream;
exports.JsonStream = JsonStream;
exports.XmlStream = XmlStream;
exports.EsQueryReader = require('./es-query-reader').EsQueryReader;
exports.EsBatchWriter = require('./es-batch-writer').EsBatchWriter;
exports.SplitterStream = require('./splitter-stream').SplitterStream;
exports.PeekStream = require('./peek-stream').PeekStream;
exports.FilterStream = require('./filter-stream').FilterStream;
exports.FlushStream = require('./flush-stream').FlushStream;