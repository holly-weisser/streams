'use strict';

var stream = require('stream');
var util = require('util');

/**
 * A Stream2 json-ifier. converts a stream of records into a json array
 * @constructor
 */
function JsonStream(opts) {
    stream.Transform.call(this);
    this._writableState.objectMode = true;
    this._readableState.objectMode = false;
    opts = opts || {};
    this.pretty = opts.pretty;
}

util.inherits(JsonStream, stream.Transform);

JsonStream.prototype._transform = function(chunk, encoding, done) {
    if (!this.started) {
        this.push('[');
        this.started = true;
    } else {
        this.push(',');
    }
    this.push(JSON.stringify(chunk, null, this.pretty && '\t'));
    done();
};

JsonStream.prototype._flush = function(done) {
    if (!this.started) {
        this.push('[');
    }
    this.push(']');
    done();
};

module.exports.JsonStream = JsonStream;