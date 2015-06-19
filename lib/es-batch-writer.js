'use strict';

var stream = require('stream');
var util = require('util');


/**
 * A Stream 2 Writable stream that writes an object or an array of objects using the ElasticSearch bulk client api.
 * This stream assumes that objects have been pre-assembled into batches upstream
 * @param client the elasticsearch client
 * @param {String} index the index to write to
 * @param {String} type the elasticsearch type
 * @constructor
 */
function EsBatchWriter(client, index, type) {
    stream.Transform.call(this, { objectMode: true });
    this.client = client;
    this.index = index;
    this.type = type;
    this.idx = 0;
}

util.inherits(EsBatchWriter, stream.Transform);

/**
 * Writes a record or a batch of records to elasticsearch using the bulk api
 * @param chunk the chunk of records to write
 * @param encoding ignored
 * @param callback invoked when the batch has been written
 * @private
 */
EsBatchWriter.prototype._write = function(chunk, encoding, callback) {
    var self = this;

    // make sure its an ar  ray
    var batch = util.isArray(chunk) ? chunk : [ chunk ];
    var body = batch.reduce(function (acc, record) {
        // increment row index counter. this should probably be done upstream by the dataset writer
        record._index = self.idx++;

        // ES bulk payload is two lines for every operation
        acc.push({ index: { _index: self.index, _type: self.type } });
        acc.push(record);

        return acc;
    }, []);

    // perform the write and notify the callback
    this.client.bulk({ body: body }, function(err, result) {
        callback(err, result);
    });
};

exports.EsBatchWriter = EsBatchWriter;