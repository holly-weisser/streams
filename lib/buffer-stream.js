'use strict';

var stream = require('stream');
var util = require('util');

/**
 * A Transform stream that transforms an object stream into a stream of buffered object arrays
 * @param {Integer} bufferSize the max size of the buffer
 * @constructor
 */
function BufferStream(bufferSize) {
    stream.Transform.call(this, { objectMode: true });
    this.bufferSize = bufferSize;
    this.buffer = [];
}

util.inherits(BufferStream, stream.Transform);

/**
 * Adds the object to the current buffer, pushing the buffer out once it grows to max capacity
 * @param object the object to buffer
 * @param encoding ignored
 * @param cb invoked once the object has been appeneded to the buffer
 * @private
 */
BufferStream.prototype._transform = function(object, encoding, cb) {
    this.buffer.push(object);
    if (this.buffer.length === this.bufferSize) {
        this.push(this.buffer);
        this.buffer = [];
    }
    cb();
};

/**
 * Flushes the remainder of the buffer
 * @param cb
 * @private
 */
BufferStream.prototype._flush = function(cb) {
    if (this.buffer.length > 0) {
        this.push(this.buffer);
    }
    cb();
};

exports.BufferStream = BufferStream;