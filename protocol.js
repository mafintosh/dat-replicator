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

var Protocol = function() {
  if (!(this instanceof Protocol)) return new Protocol()

  var self = this

  this._msbs = 2
  this._missing = 0
  this._buffer = null
  this._stream = null

  this._headerPointer = 0
  this._headerBuffer = new Buffer(50)
  this._ondrain = null

  this._cb = null
  this._nextCalled = false
  this._next = function() {
    self._nextCalled = true
    if (!self._cb) return
    var cb = self._cb
    self._cb = null
    cb()
  }

  stream.Duplex.call(this)
}

util.inherits(Protocol, stream.Duplex)

// decode

Protocol.prototype._write = function(data, enc, cb) {
  if (this._missing) return this._forward(data, cb)

  for (var i = 0; i < data.length; i++) {
    if (!(data[i] & 0x80)) this._msbs--

    this._headerBuffer[this._headerPointer++] = data[i]
    if (this._msbs) continue

    this._onheader()
    if (i === data.length-1) return cb()
    return this._write(data.slice(i), enc, cb)
  }

  cb()
}

Protocol.prototype._forward = function(data, cb) {
  if (data.length > this._missing) {
    var overflow = data.slice(this._missing)
    data = data.slice(0, this._missing)
    cb = this._rewrite(overflow, cb)
  }

  if (this._buffer) {
    data.copy(this._buffer, this._buffer.length - this._missing)
    this._missing -= data.length

    if (!this._missing) return this._onbufferdone(cb)
    return cb()
  }

  this._missing -= data.length

  if (this._missing) return this._stream.write(data, cb)

  this._stream.write(data)
  this._stream.end()
  this._onstreamdone(cb)
}

Protocol.prototype._onheader = function() {
  this._headerPointer = 0
  this._msbs = 2

  this._type = varint.decode(this._headerBuffer)
  this._missing = varint.decode(this._headerBuffer, varint.decode.bytesRead)

  switch (this._type) {
    case 0:
    case 1:
    case 2:
    return this._buffer = new Buffer(this._missing)
    case 3:
    this._nextCalled = false
    this._stream = new stream.PassThrough()
    this.emit('blob', this._stream, this._next)
    return
  }

}

Protocol.prototype._onstreamdone = function(cb) {
  this._stream = null
  if (this._nextCalled) return cb()
  this._cb = cb
}

Protocol.prototype._onbufferdone = function(cb) {
  var buf = this._buffer
  this._buffer = null

  switch (this._type) {
    case 0:
    return this.emit('meta', JSON.parse(buf.toString()), cb)
    case 1:
    return this.emit('document', JSON.parse(buf.toString()), cb)
    case 2:
    return this.emit('raw', buf, cb)
  }
}

Protocol.prototype._rewrite = function(data, cb) {
  var self = this
  return function(err) {
    if (err) return cb(err)
    self._write(data, null, cb)
  }
}

// encode

Protocol.prototype.meta = function(data, cb) {
  var buf = new Buffer(JSON.stringify(data))
  this._header(0, buf.length)
  this._push(buf, cb || noop)
}

Protocol.prototype.document = function(doc, cb) {
  var buf = new Buffer(JSON.stringify(doc))
  this._header(1, buf.length)
  this._push(buf, cb || noop)
}

Protocol.prototype.raw = function(buf, cb) {
  this._header(2, buf.length)
  this._push(buf, cb || noop)
}

Protocol.prototype.blob = function(length, cb) {
  this._header(3, length)
  return new Sink(this, cb)
}

Protocol.prototype._push = function(data, cb) {
  if (this.push(data)) cb()
  else this._ondrain = cb
}

Protocol.prototype._header = function(type, len) {
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

Protocol.prototype._read = function() {
  if (!this._ondrain) return
  var ondrain = this._ondrain
  this._ondrain = null
  ondrain()
}

module.exports = Protocol