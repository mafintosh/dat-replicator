var through = require('through2')
var protocol = require('dat-replication-protocol')
var request = require('request')
var pumpify = require('pumpify')
var pump = require('pump')
var zlib = require('zlib')
var url = require('url')

var noop = function() {}

var getKey = function(b) {
  return b.key || b.hash
}

module.exports = function(dat) {
  var that = {}
  var schema = dat.schema
  var blobs = dat.blobs

  var decodeBlobs = function(buf) {
    var blobs = schema.decode(buf, {blobsOnly:true})
    return blobs ? blobs.blobs : null
  }

  var diff = function(change, cb) {
    var latest = decodeBlobs(change.value)

    var from = change.from

    if (!latest) return cb(null, [])

    var ondone = function(latest, prev) {
      var result = []
      var keys = Object.keys(latest)

      for (var i = 0; i < keys.length; i++) {
        if (!prev || !prev[keys[i]] || getKey(prev[keys[i]]) !== getKey(latest[keys[i]])) result.push(latest[keys[i]])
      }

      cb(null, result)
    }

    if (!from) return ondone(latest, null)

    dat.get(change.key, {version:from, blobsOnly:true}, function(err, prev) {
      if (err && err.notFound) return ondone(latest, null)
      if (err) return cb(err)

      ondone(latest, prev)
    })
  }

  that.receive = function() {
    var decode = protocol.decode()
    var changes = dat.createChangesWriteStream()

    decode.blob(function(stream, cb) {
      pump(stream, dat.blobs.createWriteStream(cb))
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
    var changes = dat.createChangesReadStream({since:opts.since, data:true, live:opts.live})
    var flushed = false

    var onchange = function(change, enc, cb) {
      if (opts.blobs === false || change.subset) return encode.change(change, cb)
      diff(change, function(err, blobs) {
        if (err) return cb(err)

        var loop = function() {
          if (!blobs.length) return encode.change(change, cb)
          var next = blobs.shift()
          if (!next.size) throw new Error('blobs need .size to replicate')
          pump(dat.blobs.createReadStream(next), encode.blob(next.size, loop))
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

  that.createPullStream = function(remote, opts) {
    if (!opts) opts = {}

    var pull = pumpify()

    // request api to ping the remote
    request(remote+'/api', {json:true}, function(err, res) {
      if (err) {
        if (err.code == 'ENOTFOUND') return pull.destroy(new Error('Could not find a dat there. Are you sure the URL is right?'))
        else return pull.destroy(err)
      }
      if (res.statusCode !== 200 || typeof res.body.changes !== 'number') return pull.destroy(new Error('Invalid remote'))
      if (pull.destroyed) return

      var rcvd = that.receive(opts)
      var get = request(remote+'/api/pull', {
        headers: {
          'accept-encoding': opts.live ? '' : 'gzip'
        },
        qs: {
          live: opts.live,
          blobs: opts.blobs !== false,
          since: opts.since || dat.storage.change || 0
        }
      })

      var onerror = function(err) {
        pull.destroy(err)
      }

      var gunzip = function(res) {
        return res.headers['content-encoding'] === 'gzip' ? zlib.createGunzip() : through()
      }

      get.on('error', onerror)
      get.on('response', function(res) {
        get.removeListener('error', onerror)
        pull.stats = rcvd
        pull.setPipeline(get, gunzip(res), rcvd)
      })
    })

    pull.resume()
    return pull
  }

  that.createPushStream = function(remote, opts) {
    if (!opts) opts = {}

    var push = pumpify()

    request(remote+'/api', {json:true}, function(err, res) {
      if (err) return push.destroy(err)
      if (res.statusCode !== 200 || typeof res.body.changes !== 'number') return push.destroy(new Error('Invalid remote'))
      if (push.destroyed) return

      opts.since = res.body.changes

      var send = that.send(opts)
      var post = request.post(remote+'/api/push', {
        headers: {'content-encoding':'gzip'}
      })
      post.on('response', function(res) {
        res.resume()
        res.on('end', function() {
          if (/2\d\d/.test(res.statusCode)) return
          if (res.statusCode == 401) {
            var p = url.parse(remote)
            var remoteSuggestion =  p.protocol + '//username:password@' + p.host
            push.destroy(new Error('Unauthorized. Did you try this? \n\n  dat push ' + remoteSuggestion + '\n'))
            return
          }
          push.destroy(new Error('Remote rejected push'))
        })
      })

      push.stats = send
      push.setPipeline(send, zlib.createGzip(), post)
    })
    push.resume()
    return push
  }


  return that
}