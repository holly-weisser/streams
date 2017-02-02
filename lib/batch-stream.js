'use strict';

const stream = require('stream');
const util = require('util');

/**
 * A Transform stream that transforms an object stream into a stream of buffered object arrays
 * @param {number} bufferSize the max size of the buffer
 * @constructor
 */
class BatchStream extends stream.Transform {
    constructor (bufferSize) {
        super({ objectMode: true });
        this.bufferSize = bufferSize;
        this.buffer = [];
    }

    /**
     * Adds the object to the current buffer, pushing the buffer out once it grows to max capacity
     * @param object the object to buffer
     * @param encoding ignored
     * @param cb invoked once the object has been appeneded to the buffer
     * @private
     */
    _transform (object, encoding, cb) {
        this.buffer.push(object);
        if (this.buffer.length === this.bufferSize) {
            this.push(this.buffer);
            this.buffer = [];
        }
        cb();
    }

    _flush(cb) {
        if (this.buffer.length > 0) {
            this.push(this.buffer);
        }
        cb();
    }
}

exports.BatchStream = BatchStream;