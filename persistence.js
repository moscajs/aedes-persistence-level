'use strict'

const Qlobber = require('qlobber').Qlobber
const Packet = require('aedes-packet')
const through = require('through2')
const msgpack = require('msgpack-lite')
const callbackStream = require('callback-stream')
const pump = require('pump')
const EventEmitter = require('events').EventEmitter
const inherits = require('util').inherits
const multistream = require('multistream')

const QlobberOpts = {
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/',
  match_empty_levels: true
}
const RETAINED = 'retained:'
const SUBSCRIPTIONS = 'subscriptions:'
const OUTGOING = 'outgoing:'
const OUTGOINGID = 'outgoing-id:'
const INCOMING = 'incoming:'
const WILL = 'will:'
const encodingOption = {
  valueEncoding: 'binary'
}

function createValueStream (db, start) {
  return db.createValueStream(Object.assign({
    gt: start,
    lt: start + '\xff'
  }, encodingOption))
}

function LevelPersistence (db) {
  if (!(this instanceof LevelPersistence)) {
    return new LevelPersistence(db)
  }

  this._db = db
  this._trie = new Qlobber(QlobberOpts)
  this._ready = false

  const trie = this._trie
  const that = this

  pump(createValueStream(this._db, SUBSCRIPTIONS), through.obj(function (blob, enc, cb) {
    const chunk = msgpack.decode(blob)
    trie.add(chunk.topic, chunk)
    cb()
  }), function (err) {
    if (err) {
      that.emit('error', err)
      return
    }
    that._ready = true
    that.emit('ready')
  })
}

inherits(LevelPersistence, EventEmitter)

LevelPersistence.prototype.storeRetained = function (packet, cb) {
  const key = `${RETAINED}${packet.topic}`
  if (packet.payload.length === 0) {
    this._db.del(key, cb)
  } else {
    this._db.put(key, msgpack.encode(packet), cb)
  }
}

LevelPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  const that = this
  const streams = patterns.map(function (p) {
    return that.createRetainedStream(p)
  })
  return multistream.obj(streams)
}

LevelPersistence.prototype.createRetainedStream = function (pattern) {
  const qlobber = new Qlobber(QlobberOpts)
  qlobber.add(pattern, true)

  const res = through.obj(function (blob, encoding, deliver) {
    const packet = msgpack.decode(blob)
    if (qlobber.match(packet.topic).length) {
      deliver(null, packet)
    } else {
      deliver()
    }
  })

  pump(createValueStream(this._db, RETAINED), res)

  return res
}

function withClientId (sub) {
  return {
    topic: sub.topic,
    clientId: this.id,
    qos: sub.qos
  }
}

function toSubKey (sub) {
  return `${SUBSCRIPTIONS}${sub.clientId}:${sub.topic}`
}

function addSubToTrie (sub) {
  let add
  const matched = this.match(sub.topic)
  if (matched.length > 0) {
    add = true
    for (let i = 0; i < matched.length; i++) {
      if (matched[i].clientId === sub.clientId) {
        if (matched[i].qos === sub.qos) {
          add = false
          break
        } else {
          this.remove(matched[i].topic, matched[i])
          if (sub.qos === 0) {
            add = false
          }
        }
      }
    }
  } else if (sub.qos > 0) {
    add = true
  }

  if (add) {
    this.add(sub.topic, sub)
  }

  return sub
}

LevelPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  if (!this._ready) {
    this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
    return
  }

  subs = subs
    .map(withClientId, client)
    .map(addSubToTrie, this._trie)
  const opArray = []
  subs.forEach(function (sub) {
    const ops = {}
    ops.type = 'put'
    ops.key = toSubKey(sub)
    ops.value = msgpack.encode(sub)
    opArray.push(ops)
  })

  this._db.batch(opArray, function (err) {
    cb(err, client)
  })
}

function toSubObj (topic) {
  return {
    clientId: this.id,
    topic: topic
  }
}

function delSubFromTrie (sub) {
  this
    .match(sub.topic)
    .filter(matching, sub)
    .forEach(rmSub, this)

  return sub
}

function matching (sub) {
  return sub.topic === this.topic && sub.clientId === this.clientId
}

function rmSub (sub) {
  this.remove(sub.topic, sub)
}

LevelPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
  subs = subs
    .map(toSubObj, client)
    .map(delSubFromTrie, this._trie)
  const opArray = []
  subs.forEach(function (sub) {
    const ops = {}
    ops.type = 'del'
    ops.key = toSubKey(sub)
    ops.value = msgpack.encode(sub)
    opArray.push(ops)
  })
  this._db.batch(opArray, function (err) {
    cb(err, client)
  })
}

function rmClientId (sub) {
  return {
    topic: sub.topic,
    qos: sub.qos
  }
}

LevelPersistence.prototype.subscriptionsByClient = function (client, cb) {
  createValueStream(this._db, `${SUBSCRIPTIONS}${client.id}`)
    .pipe(callbackStream({ objectMode: true }, function (err, blob) {
      const subs = blob.map(msgpack.decode)
      let resubs = subs.map(rmClientId)
      if (resubs.length === 0) {
        resubs = null
      }
      cb(err, resubs, client)
    }))
}

LevelPersistence.prototype.countOffline = function (cb) {
  let clients = 0
  let subs = 0
  let lastClient = null
  pump(createValueStream(this._db, SUBSCRIPTIONS), through.obj(function (blob, enc, cb) {
    const sub = msgpack.decode(blob)
    if (lastClient !== sub.clientId) {
      lastClient = sub.clientId
      clients++
    }
    if (sub.qos > 0) {
      subs++
    }
    cb()
  }), function (err) {
    cb(err, subs, clients)
  })
}

LevelPersistence.prototype.subscriptionsByTopic = function (pattern, cb) {
  if (!this._ready) {
    this.once('ready', this.subscriptionsByTopic.bind(this, pattern, cb))
    return this
  }
  cb(null, this._trie.match(pattern))
}

LevelPersistence.prototype.cleanSubscriptions = function (client, cb) {
  const that = this
  this.subscriptionsByClient(client, function (err, subs) {
    if (err || !subs) { return cb(err, client) }

    that.removeSubscriptions(client, subs.map(function (sub) {
      return sub.topic
    }), function (err) {
      cb(err, client)
    })
  })
}

LevelPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
  const key = `${OUTGOING}${sub.clientId}:${packet.brokerId}:${packet.brokerCounter}`
  this._db.put(key, msgpack.encode(new Packet(packet)), cb)
}

LevelPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
  if (!subs || subs.length === 0) {
    return cb(null, packet)
  }
  let count = 0
  for (let i = 0; i < subs.length; i++) {
    this.outgoingEnqueue(subs[i], packet, function (err) {
      if (!err) {
        count++
        if (count === subs.length) {
          cb(null, packet)
        }
      } else {
        cb(err)
      }
    })
  }
}

function updateWithBrokerData (that, client, packet, cb) {
  const prekey = `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
  const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`

  that._db.get(prekey, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      cb(new Error('no such packet'), client, packet)
      return
    } else if (err) {
      cb(err, client, packet)
      return
    }

    const decoded = msgpack.decode(blob)

    if (decoded.messageId > 0) {
      that._db.del(`${OUTGOINGID}${client.id}:${decoded.messageId}`)
    }

    that._db.put(postkey, msgpack.encode(packet), function (err) {
      if (err) {
        cb(err, client, packet)
      } else {
        that._db.put(prekey, msgpack.encode(packet), function (err) {
          cb(err, client, packet)
        })
      }
    })
  })
}

function augmentWithBrokerData (that, client, packet, cb) {
  const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`
  that._db.get(postkey, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      return cb(new Error('no such packet'))
    } else if (err) {
      return cb(err)
    }

    const decoded = msgpack.decode(blob)

    packet.brokerId = decoded.brokerId
    packet.brokerCounter = decoded.brokerCounter
    cb(null)
  })
}

LevelPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  const that = this
  if (packet.brokerId) {
    updateWithBrokerData(this, client, packet, cb)
  } else {
    augmentWithBrokerData(this, client, packet, function (err) {
      if (err) { return cb(err, client, packet) }

      updateWithBrokerData(that, client, packet, cb)
    })
  }
}

LevelPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
  const that = this
  const key = `${OUTGOINGID}${client.id}:${packet.messageId}`
  this._db.get(key, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      return cb(null)
    } else if (err) {
      return cb(err)
    }

    const packet = msgpack.decode(blob)
    const prekey = `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
    const batch = that._db.batch()
    batch.del(key)
    batch.del(prekey)
    batch.write(function (err) {
      cb(err, packet)
    })
  })
}

LevelPersistence.prototype.outgoingStream = function (client) {
  const key = `${OUTGOING}${client.id}`
  return pump(createValueStream(this._db, key), through.obj(function (blob, enc, cb) {
    cb(null, msgpack.decode(blob))
  }))
}

LevelPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  const key = `${INCOMING}${client.id}:${packet.messageId}`
  const newp = new Packet(packet)
  newp.messageId = packet.messageId
  this._db.put(key, msgpack.encode(newp), cb)
}

LevelPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  const key = `${INCOMING}${client.id}:${packet.messageId}`
  this._db.get(key, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      cb(new Error('no such packet'), client)
    } else if (err) {
      cb(err, client)
    } else {
      cb(null, msgpack.decode(blob), client)
    }
  })
}

LevelPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  const key = `${INCOMING}${client.id}:${packet.messageId}`
  this._db.del(key, cb)
}

LevelPersistence.prototype.putWill = function (client, packet, cb) {
  const key = `${WILL}${client.id}`

  packet.brokerId = this.broker.id
  packet.clientId = client.id

  this._db.put(key, msgpack.encode(packet), function (err) {
    cb(err, client)
  })
}

LevelPersistence.prototype.getWill = function (client, cb) {
  const key = `${WILL}${client.id}`
  this._db.get(key, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      cb(null, null, client)
    } else {
      cb(err, msgpack.decode(blob), client)
    }
  })
}

LevelPersistence.prototype.delWill = function (client, cb) {
  const key = `${WILL}${client.id}`
  const that = this
  this._db.get(key, encodingOption, function (err, blob) {
    if (err) {
      return cb(err, null, client)
    }
    that._db.del(key, function (err) {
      cb(err, msgpack.decode(blob), client)
    })
  })
}

LevelPersistence.prototype.streamWill = function (brokers) {
  const valueStream = createValueStream(this._db, WILL)

  if (!brokers) {
    return pump(valueStream, through.obj(function (blob, enc, cb) {
      cb(null, msgpack.decode(blob))
    }))
  }

  return pump(valueStream, through.obj(function (blob, enc, cb) {
    const chunk = msgpack.decode(blob)
    if (!brokers[chunk.brokerId]) {
      this.push(chunk)
    }
    cb()
  }))
}

LevelPersistence.prototype.getClientList = function (topic) {
  const valueStream = createValueStream(this._db, SUBSCRIPTIONS)

  return pump(valueStream, through.obj(function (blob, enc, cb) {
    const chunk = msgpack.decode(blob)
    if (chunk.topic === topic) {
      this.push(chunk.clientId)
    }
    cb()
  }))
}

LevelPersistence.prototype.destroy = function (cb) {
  this._db.close(cb)
}

module.exports = LevelPersistence
