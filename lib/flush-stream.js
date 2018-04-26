'use strict';

var uuid = require('uuid');
var stream = require('stream');
var util = require('util');
var async = require('async');

class FlushStream extends stream.Transform {

    constructor(client) {
        super({objectMode: true});
        this.client = client;
        this.id = uuid.v1();
        this.count = 0;
        this.on('end', () => this.client.del(this.id));
        this.on('error', () => this.client.del(this.id));
    }

    _transform(object, encoding, done) {//jshint ignore:line
        var records = [].concat(object);
        this.count += records.length;

        records.reduce((b, r)=> b.rpush([this.id, JSON.stringify(r)]), this.client.multi())
            .exec(err => done(err));
    }

    _flush(done) {
        var self = this;

        var iterator = function (done) {
            self.client.lpop([self.id], function (err, value) {
                if(err) return done(err);
                self.push(JSON.parse(value));
                done();
            });
        };

        async.whilst(()=>this.count--, iterator, err => done(err));

    }
}

exports.FlushStream = FlushStream;
