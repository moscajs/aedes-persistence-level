'use strict'

var level = require('level')
var aedesPersistencelevel = require('.')
var net = require('net')
var port = 1883

var aedes = require('aedes')({
  persistence: aedesPersistencelevel(level('./mydb'))
})
var server = net.createServer(aedes.handle)

server.listen(port, function () {
  console.log('server listening on port', port)
})

aedes.on('clientError', function (client, err) {
  console.log('client error', client.id, err.message, err.stack)
})

aedes.on('publish', function (packet, client) {
  if (client) {
    console.log('message from client', client.id)
  }
})

aedes.on('client', function (client) {
  console.log('new client', client.id)
})
