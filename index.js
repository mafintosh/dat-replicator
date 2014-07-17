var protocol = require('dat-replication-protocol')
var through = require('through2')
var request = require('request')
var debug = require('debug')('dat-replicator')

var STREAM_OPTS = {highWaterMark:16}

var replication = function(dat) {
  var that = {}

  that.createPullStream = function(remote, opts) {
    var rcvd = that.receive(opts)
    var req = request.post(remote+'/api/replicator/send')
    req.pipe(rcvd).pipe(req)
    return rcvd
  }

  that.createPushStream = function(remote, opts) {
    var send = that.send(opts)
    var req = request.post(remote+'/api/replicator/receive')
    req.pipe(send).pipe(req)
    return send
  }

  that.send = function(opts) {
    if (!opts) opts = {}
    var p = protocol()

    var writeAttachments = function(attachments, cb) {
      if (!attachments) return cb()

      var keys = Object.keys(attachments)
      var loop = function(err) {
        if (err) return cb(err)
        if (!keys.length) return cb()
        var bl = attachments[keys.shift()]
        debug('send.blob')
        dat.blobs.createReadStream(bl.hash).pipe(p.blob(bl.size, loop))
      }

      loop()
    }

    var writeNoBlobs = function(doc, enc, cb) {
      var data = doc.value || doc.data
      debug('send.document', data)
      p.document(data, cb)
    }

    var write = function(doc, enc, cb) {
      var data = doc.value || doc.data
      writeAttachments(data.attachments, function(err) {
        if (err) return cb(err)
        debug('send.document', data)
        p.document(data, cb)
      })
    }

    var flush = function(cb) {
      debug('send.finalize')
      p.finalize(cb)
    }

    var ready = function(meta) {
      var rs = dat.createChangesStream({
        since: meta.change,
        data: true
      })

      debug('receive.meta', meta)
      rs.pipe(through.obj(STREAM_OPTS, meta.blobs === false ? writeNoBlobs : write, flush))
    }

    if (opts.meta) ready(opts.meta)
    else {
      p.once('meta', function(meta, cb) {
        ready(meta)
        cb()
      })
    }

    if (opts.ping !== false) p.ping() // support more transports by forcing a flush (looking at you http)

    // not sure about this but this is a start...
    p.on('conflict', function(conflict, cb) {
      debug('receive.conflict', conflict)
      var err = new Error(conflict.message || 'conflict')
      err.conflict = true
      err.key = conflict.key
      err.version = conflict.version
      p.emit('error', err)
      cb()
    })

    return p
  }

  that.receive = function(opts) {
    if (!opts) opts = {}

    var p = protocol()
    var ws = dat.createWriteStream({objects:true})

    if (opts.meta !== false) {
      var meta = {
        blobs: opts.blobs,
        change: dat.storage.change,
        schema: dat.schema.toJSON()
      }

      debug('send.meta', meta)
      p.meta(meta)
    }

    ws.on('error', function(err) {
      p.emit('error', err)
    })
    
    ws.on('conflict', function(conflict) {
      debug('send.conflict', conflict)
      p.conflict({message:conflict.message, key:conflict.key, version:conflict.version})
    })

    p.on('blob', function(blob, cb) {
      debug('receive.blob')
      blob.pipe(dat.blobs.createWriteStream(cb))
    })

    p.on('document', function(doc, cb) {
      debug('receive.document', doc)
      ws.write(doc, cb)
    })

    p.on('finalize', function(cb) {
      debug('receive.finalize')
      ws.on('end', cb)
      ws.end()
    })

    return p
  }

  return that
}

module.exports = replication