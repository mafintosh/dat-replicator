var protocol = require('dat-replication-protocol')
var through = require('through2')
var request = require('request')

var STREAM_OPTS = {highWaterMark:16}

var progressStream = function(p) {
  var progress = through.obj()
  p.on('transfer', function(type) {
    if (type === 'protobuf' || type === 'document') progress.write({type:type})
  })
  p.on('error', function(err) {
    progress.emit('error', err)
  })
  p.on('end', function() {
    progress.end()
  })
  return progress
}

var replication = function(dat) {
  var that = {}

  that.createPullStream = function(remote) {
    var rcvd = that.receive()
    var req = request.post(remote+'/api/replicator/send')
    req.pipe(rcvd).pipe(req)
    return progressStream(rcvd)
  }

  that.createPushStream = function(remote) {
    var send = that.send()
    var req = request.post(remote+'/api/replicator/receive')
    req.pipe(send).pipe(req)
    return progressStream(send)
  }

  that.send = function(opts) {
    if (!opts) opts = {}
    var p = protocol()

    var writeAttachments = function(attachments, cb) {
      if (!attachments) return cb()

      var keys = Object.keys(attachments)
      var loop = function() {
        if (!keys.length) return cb()
        var bl = attachments[keys.shift()]
        dat.blobs.createReadStream(bl.hash).pipe(p.blob(bl.size, cb))
      }

      loop()
    }

    var write = function(doc, enc, cb) {
      writeAttachments(doc.data.attachments, function(err) {
        if (err) return cb(err)
        p.document(doc.data, cb)
      })
    }

    var flush = function(cb) {
      p.finalize()
      cb()
    }

    var ready = function(meta) {
      var rs = dat.createChangesStream({
        since: meta.change,
        data: true
      })

      rs.pipe(through.obj(STREAM_OPTS, write, flush))
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
      p.meta({
        change: dat.storage.change,
        schema: dat.schema.toJSON()
      })
    }

    ws.on('error', function(err) {
      if (err.conflict) return p.conflict({message:err.message, key:err.key, version:err.version})
      p.emit('error', err)
    })

    ws.on('end', function() {
      p.finalize()
    })

    p.on('blob', function(blob, cb) {
      blob.pipe(dat.blobs.createWriteStream(cb))
    })

    p.on('document', function(doc, cb) {
      ws.write(doc, cb)
    })

    p.on('finish', function() {
      ws.end()
    })

    return p
  }

  return that
}

module.exports = replication