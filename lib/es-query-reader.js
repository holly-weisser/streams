'use strict';

var util = require('util');
var stream = require('stream');
var _ = require('lodash');

function EsQueryReader(client, index, type, query, bufferSize){
    stream.Readable.call(this, {objectMode: true});

    this.client = client;
    this.index = index;
    this.type = type;
    this.query = query;
    this.bufferSize = bufferSize || 50;
    this.buf = [];
    this.loading = false;
    this.from = 0;
    this.total = 0;
}

util.inherits(EsQueryReader, stream.Readable);

EsQueryReader.prototype.nextBatch = function() {
    var self = this;

    return this.client.search({
        index: this.index,
        type: this.type,
        body: _.merge({}, this.query, { from: this.from, size: this.bufferSize })
    })
        .then(function (result) {
            self.buf = _.pluck(result.hits.hits, '_source');
            self.from += result.hits.hits.length;
            self.loading = false;
        })
        .catch(function(err){
            self.loading = false;
            self.emit('error', err);
        });
};

EsQueryReader.prototype._read = function () {
    var self = this;

    if (this.buf.length > 0) return this.push(this.buf.shift());

    if (this.loading) return;

    this.nextBatch()
        .then(function () {
            if (self.buf.length > 0) {
                self.push(self.buf.shift());
            } else {
                self.push(null);
            }
        });
};

EsQueryReader.prototype.cancel = function () {
    this.canceled = true;
    this.loading = false;
    this.buf = [];
    this.push(null);
};

module.exports.EsQueryReader = EsQueryReader;