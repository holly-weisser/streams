'use strict';

var _ = require('lodash');
var stream = require('stream');
var through2 = require('through2');
var P = require('bluebird');

exports.readArray = function(array) {
    var index = 0;
    var s = new stream.Readable({ objectMode: true });
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
    var res = [];
    var s = new stream.Writable({ objectMode: true });
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
        var writeStream = new stream.Writable();
        var buffers = [];
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
    return through2.obj(function (record, enc, done) {
        try {
            done(null, mapFn(record));
        } catch (err) {
            done(err);
        }
    }, flushFn);
};

exports.map = function(mapFn, flushFn) {
    return through2.obj(function (record, enc, done) {
        try {
            mapFn(record, done);
        } catch (err) {
            done(err);
        }
    }, flushFn);
};

exports.eachSync = function(eachFn, flushFn) {
    return through2.obj(function (record, enc, done) {
        try {
            eachFn(record);
            done(null, record);
        } catch (err) {
            done(err);
        }
    }, flushFn);
};

exports.each = function(eachFn, flushFn) {
    return through2.obj(function (record, enc, done) {
        try {
            eachFn(record, function (err) {
                done(err, record);
            });
        } catch (err) {
            done(err);
        }
    }, flushFn);
};

exports.writeObject = function (done) {
    var result;

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
    var writeStream = new stream.Writable(options);
    writeStream._write = function(record, enc, done) {
        done();
    };

    return writeStream;
};

exports.pipeline = function(streams) {
    return streams.reduce(function (prev, next) {
        return prev.pipe(next);
    });
};

exports.BufferStream = require('./buffer-stream').BufferStream;
exports.JsonStream = require('./json-stream').JsonStream;
exports.XmlStream = require('./xml-stream').XmlStream;
exports.EsQueryReader = require('./es-query-reader').EsQueryReader;
exports.EsBatchWriter = require('./es-batch-writer').EsBatchWriter;
exports.SplitterStream = require('./splitter-stream').SplitterStream;