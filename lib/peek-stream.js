'use strict';

var P = require('bluebird');
var stream = require('stream');

/**
 * An object-mode stream, which is paused until a callback function can perform a (possibly async) side-effecty action
 * based on the first record of the stream
 */
class PeekStream extends stream.Transform {
    constructor(onRecord) {
        super({ objectMode: true });

        this.onRecord = onRecord;
    }

    _transform(record, enc, done) {
        if (this.initialized) return done(null, record);

        P.resolve(record)
            .tap(r => this.onRecord(r))
            .then(r => {
                this.initialized = true;
                done(null, r);
            })
            .catch(err => this.emit('error', err));
    }
}

exports.PeekStream = PeekStream;