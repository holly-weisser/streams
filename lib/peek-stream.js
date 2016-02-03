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

    /**
     * Asynchronously peeks at the first record of the stream
     * @param r
     * @return {*}
     * @private
     */
    _maybePeek(r) {
        this._initval = this._initval || this.onRecord(r);
        return this._initval;
    }

    _transform(record, enc, done) {
        return P.resolve(record)
            .tap(r => this._maybePeek(r))
            .then(r => done(null, r))
            .catch(err => this.emit('error', err));
    }
}

exports.PeekStream = PeekStream;