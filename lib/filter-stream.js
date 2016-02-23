'use strict';

var _ = require('lodash');
var Transform = require('stream').Transform;

class FilterStream extends Transform {
    constructor(predicate) {
        super({ objectMode: true });
        this.predicate = predicate || _.constant(true);
    }
    _transform(record, enc, done) {
        try {
            return this.predicate(record) ? done(null, record) : done();
        } catch (err) {
            done(err);
        }
    }
}

exports.FilterStream = FilterStream;