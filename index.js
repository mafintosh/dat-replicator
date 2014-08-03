var through = require('through2')
var protocol = require('dat-replication-protocol')
var pump = require('pump')

var noop = function() {}

module.exports = function(db, store) {
  var that = {}

  var blobs = db.subset('blobs')

  var diff = function(change, cb) {
    var latest = JSON.parse(change.value.toString())
    var from = change.from

    if (!from) return cb(null, latest)

    blobs.get(key, {version:from, valueEncoding:'json'}, function(err, prev) {
      if (err && err.notFound) return cb(null, latest)
      if (err) return cb(err)

      latest = latest.filter(function(bl) {
        for (var i = 0; i < prev.length; i++) {
          if (prev[i].hash === bl.hash) return false
        }
        return true
      })

      cb(null, latest)
    })
  }

  that.createPullStream = function(remote, opts) {
    if (!opts) opts = {}

    var rcvd = that.receive(opts)
    var get = request(remote+'/api/changes', {
      qs: {
        blobs: opts.blobs !== false,
        since: opts.since || 0,
        type: 'binary'
      }
    })

    return pump(get, rcvd)
  }

  that.createPushStream = function(remote, opts) {
    if (!opts) opts = {}

    var send = that.send(opts)
    var post = request.post(remote+'/api/changes', {
      qs: {
        type: 'binary'
      }
    })

    post.on('response', function(res) {
      if (!/2\d\d/.test(res.statusCode)) return
      res.resume()
      res.on('end', function() {
        send.destroy(new Error('Remote rejected push'))
      })
    })

    pump(send, post)
    return send
  }

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

      diff(change, function(err, blobs) {
        if (err) return cb(err)

        var loop = function() {
          if (!blobs.length) return encode.change(change, cb)
          var next = blobs.shift()
          pump(store.createReadStream(next.hash), encode.blob(next.size, loop))
        }

        loop()
      })
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