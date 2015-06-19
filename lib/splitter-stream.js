'use strict';

var stream = require('stream');
var util = require('util');

function SplitterStream(){
    stream.Transform.call(this, {objectMode: true});
}
util.inherits(SplitterStream, stream.Transform);

SplitterStream.prototype._transform = function (chunk, enc, next) {
    for(var i = 0; i<chunk.length;i++) {
        this.push(chunk[i]);
    }
    next();
};

module.exports.SplitterStream = SplitterStream;