# dat-replicator

Replicate a dat instance to another one (in a smart way) using nothing but streams

```
npm install dat-replicator
```

## Usage

Replicating is a simple as piping a few streams

``` js
var replicator = require('dat-replicator')
var r1 = replicator(dat1)
var r2 = replicator(dat2)

var send = r1.send()
var rcvd = r2.receive()

send.pipe(rcvd).pipe(send) // will sync dat1 to dat2
```

## Real world example

First setup a dat instance to receive data. Using tcp this could look like

``` js
var replicator = require('dat-replicator')
var net = require('net')

var r = replicator(datReceive) // datReceive is a dat instance

net.createServer(function(socket) {
  socket.pipe(r.receive()).pipe(socket)
}).listen(9090)
```

Then setup another that instance to send the data. Again using tcp

``` js
var r = replication(datSend) // datSend is a dat instance
var socket = net.connect(9090)

socket.pipe(r.send()).pipe(socket)
```

Thats it! All of the changes that `datSend` have and that `datReceive` doesn't will
be inserted into `datReceive`.

## Metadata

Per default metadata (the change count etc) is exchanged over the stream as well.
If for some reason you already have this do

``` js
var send = r1.send({meta:{change:100}})
var rcvd = r2.receive({meta:false})

send.pipe(rcvd).pipe(send)
```

This will disable the metadata exchange

## License

MIT