var through = require('through2')
var protocol = require('dat-replication-protocol')
var pump = require('pump')

var noop = function() {}

module.exports = function(db, store) {
  var that = {}

  that.receive = function() {
    var decode = protocol.decode()
    var changes = db.createChangesWriteStream({valueEncoding:'binary'})

    decode.blob(function(stream, cb) {
      pump(stream, store.createWriteStream(), cb)
    })

    decode.change(function(change, cb) {
      changes.write(change, cb)
    })

    decode.finalize(function(cb) {
      changes.end(cb)
    })

    return decode
  }

  that.send = function(opts) {
    if (!opts) opts = {}

    var encode = protocol.encode()
    var changes = db.createChangesReadStream({since:opts.since, valueEncoding:'binary', data:true})
    var flushed = false

    var onchange = function(change, enc, cb) {
      if (opts.blobs === false || change.subset !== 'blobs') return encode.change(change, cb)

      var metadata = JSON.parse(data.value.toString())
      if (!data.from) metadata = metadata.slice(-1)

      var loop = function() {
        if (!metadata.length) return encode.change(change, cb)

        var next = metadata.shift()
        pump(store.createReadStream(next.hash), encode.blob(next.size, loop))
      }

      loop()
    }

    var onflush = function() {
      flushed = true
      encode.finalize()
    }

    changes.pipe(through.obj(onchange, onflush))
    encode.on('close', function() {
      if (!flushed) changes.destroy()
    })

    return encode
  }

  return that
}