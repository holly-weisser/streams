'use strict';

const stream = require('stream');
const util = require('util');

/**
 * A Transform stream that transforms an object stream into a stream of buffered object arrays
 * @param {number} bufferSize the max size of the buffer
 * @constructor
 */
class BufferStream extends stream.Transform {
    constructor (bufferSize, opts) {
        super(opts);
        this.bufferSize = bufferSize;
    }

    /**
     * Adds the object to the current buffer, pushing the buffer out once it grows to max capacity
     * @param chunk the object to buffer
     * @param encoding ignored
     * @param cb invoked once the object has been appeneded to the buffer
     * @private
     */
    _transform (chunk, encoding, cb) {
        this.chunk = this.chunk ? Buffer.concat([ this.chunk, chunk ], this.chunk.length + chunk.length ) : chunk;
        if (this.chunk.length > this.bufferSize) {
            this.push(this.chunk);
            delete this.chunk;
        }
        cb();
    }

    _flush(cb) {
        if (this.chunk) {
            this.push(this.chunk);
        }
        cb();
    }
}

exports.BufferStream = BufferStream;