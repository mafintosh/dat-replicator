var stream = require('stream')
var varint = require('varint')
var util = require('util')

var Decode = function() {
  if (!(this instanceof Decode)) return new Decode()

  var self = this

  this._msbs = 2
  this._missing = 0
  this._buffer = null
  this._stream = null

  this._headerPointer = 0
  this._header = new Buffer(50)

  this._cb = null
  this._nextCalled = false
  this._next = function() {
    self._nextCalled = true
    if (!self._cb) return
    var cb = self._cb
    self._cb = null
    cb()
  }

  stream.Writable.call(this)
}

util.inherits(Decode, stream.Writable)

Decode.prototype._write = function(data, enc, cb) {
  if (this._missing) return this._forward(data, cb)

  for (var i = 0; i < data.length; i++) {
    if (!(data[i] & 0x80)) this._msbs--

    this._header[this._headerPointer++] = data[i]
    if (this._msbs) continue

    this._onheader()
    if (i === data.length-1) return cb()
    return this._write(data.slice(i), enc, cb)
  }

  cb()
}

Decode.prototype._forward = function(data, cb) {
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

Decode.prototype._onheader = function() {
  this._headerPointer = 0
  this._msbs = 2

  this._type = varint.decode(this._header)
  this._missing = varint.decode(this._header, varint.decode.bytesRead)

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

Decode.prototype._onstreamdone = function(cb) {
  this._stream = null
  if (this._nextCalled) return cb()
  this._cb = cb
}

Decode.prototype._onbufferdone = function(cb) {
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

Decode.prototype._rewrite = function(data, cb) {
  var self = this
  return function(err) {
    if (err) return cb(err)
    self._write(data, null, cb)
  }
}

module.exports = Decode