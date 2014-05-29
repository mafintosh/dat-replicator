var stream = require('stream')
var varint = require('varint')
var util = require('util')

var pool = new Buffer(5012)
var used = 0
var noop = function() {}

var Sink = function(parent, cb) {
  this._parent = parent
  if (cb) this.once('finish', cb)
  stream.Writable.call(this)
}

util.inherits(Sink, stream.Writable)

Sink.prototype._write = function(data, enc, cb) {
  this._parent._push(data, cb)
}

var Encode = function() {
  if (!(this instanceof Encode)) return new Encode()
  this._ondrain = null
  stream.Readable.call(this)
}

util.inherits(Encode, stream.Readable)

Encode.prototype.meta = function(data, cb) {
  var buf = new Buffer(JSON.stringify(data))
  this._header(0, buf.length)
  this._push(buf, cb || noop)
}

Encode.prototype.document = function(doc, cb) {
  var buf = new Buffer(JSON.stringify(doc))
  this._header(1, buf.length)
  this._push(buf, cb || noop)
}

Encode.prototype.raw = function(buf, cb) {
  this._header(2, buf.length)
  this._push(buf, cb || noop)
}

Encode.prototype.blob = function(length, cb) {
  this._header(3, length)
  return new Sink(this, cb)
}

Encode.prototype._push = function(data, cb) {
  if (this.push(data)) cb()
  else this._ondrain = cb
}

Encode.prototype._header = function(type, len) {
  if (pool.length - used < 50) {
    used = 0
    pool = new Buffer(5012)
  }

  var start = used

  varint.encode(type, pool, used)
  used += varint.encode.bytesWritten

  varint.encode(len, pool, used)
  used += varint.encode.bytesWritten

  this.push(pool.slice(start, used))
}

Encode.prototype._read = function() {
  if (!this._ondrain) return
  var ondrain = this._ondrain
  this._ondrain = null
  ondrain()
}

module.exports = Encode