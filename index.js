var through = require('through2')
var protocol = require('dat-replication-protocol')
var pump = require('pump')

var noop = function() {}

module.exports = function(db, store) {
  var that = {}

  that.receive = function() {
    var blobs = 0
    var next = noop

    var onblob = function(stream) {
      blobs++
      pump(stream, store.createWriteStream(), function() {
        if (--blobs) return
        next()
        next = noop
      })
    }

    var ondrain = function(cb) {
      if (!blobs) cb()
      else next = cb
    }

    var readBlobs = function(data, enc, cb) {
      ondrain(function() {
        cb(null, data)
      })
    }

    var onchanges = function(stream) {
      pump(stream, through.obj(readBlobs), db.createChangesWriteStream({valueEncoding:'binary'}))
    }

    return protocol(function(type, stream) {
      if (type === protocol.CHANGES) onchanges(stream)
      if (type === protocol.BLOB) onblob(stream)
    })
  }

  that.send = function(opts) {
    if (!opts) opts = {}

    var p = protocol()

    var writeBlobs = function(data, enc, cb) {
      if (opts.blobs === false) return cb(null, data)
      if (data.subset !== 'blobs') return cb(null, data)

      var metadata = JSON.parse(data.value.toString())
      if (!data.from) metadata = metadata.slice(-1)

      var loop = function() {
        if (!metadata.length) return cb(null, data)

        var next = metadata.shift()
        var bl = store.createReadStream(next.hash)

        pump(bl, p.createBlobStream(next.size)).on('finish', loop)
      }

      loop()
    }

    var finalize = function() {
      p.finalize()
    }

    pump(
      db.createChangesReadStream({since:opts.since, valueEncoding:'binary', data:true}),
      through.obj(writeBlobs),
      p.createChangesStream().on('finish', finalize)
    )

    return p
  }

  return that
}