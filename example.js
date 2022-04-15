const { Level } = require('level')
const aedesPersistencelevel = require('.')
const net = require('net')
const port = 1883

const aedes = require('aedes')({
  persistence: aedesPersistencelevel(new Level('./mydb'))
})
const server = net.createServer(aedes.handle)

server.listen(port, () => {
  console.log('server listening on port', port)
})

aedes.on('clientError', (client, err) => {
  console.log('client error', client.id, err.message, err.stack)
})

aedes.on('publish', (packet, client) => {
  if (client) {
    console.log('message from client', client.id)
  }
})

aedes.on('client', client => {
  console.log('new client', client.id)
})
