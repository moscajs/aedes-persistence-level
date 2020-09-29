'use strict'

var Qlobber = require('qlobber').Qlobber
var Packet = require('aedes-packet')
var through = require('through2')
var msgpack = require('msgpack-lite')
var callbackStream = require('callback-stream')
var pump = require('pump')
var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var multistream = require('multistream')

var QlobberOpts = {
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/',
  match_empty_levels: true
}
var RETAINED = 'retained:'
var SUBSCRIPTIONS = 'subscriptions:'
var OUTGOING = 'outgoing:'
var OUTGOINGID = 'outgoing-id:'
var INCOMING = 'incoming:'
var WILL = 'will:'
var encodingOption = {
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

  var trie = this._trie
  var that = this

  pump(createValueStream(this._db, SUBSCRIPTIONS), through.obj(function (blob, enc, cb) {
    var chunk = msgpack.decode(blob)
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
  if (packet.payload.length === 0) {
    this._db.del(RETAINED + packet.topic, cb)
  } else {
    this._db.put(RETAINED + packet.topic, msgpack.encode(packet), cb)
  }
}

LevelPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  var that = this
  var streams = patterns.map(function (p) {
    return that.createRetainedStream(p)
  })
  return multistream.obj(streams)
}

LevelPersistence.prototype.createRetainedStream = function (pattern) {
  var qlobber = new Qlobber(QlobberOpts)
  qlobber.add(pattern, true)

  var res = through.obj(function (blob, encoding, deliver) {
    var packet = msgpack.decode(blob)
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
  return SUBSCRIPTIONS + sub.clientId + ':' + sub.topic
}

function addSubToTrie (sub) {
  var add
  var matched = this.match(sub.topic)
  if (matched.length > 0) {
    add = true
    for (var i = 0; i < matched.length; i++) {
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
  let opArray = []
  subs.forEach(function (sub) {
    let ops = {}
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
  let opArray = []
  subs.forEach(function (sub) {
    let ops = {}
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
  createValueStream(this._db, SUBSCRIPTIONS + client.id)
    .pipe(callbackStream({ objectMode: true }, function (err, blob) {
      var subs = blob.map(msgpack.decode)
      var resubs = subs.map(rmClientId)
      if (resubs.length === 0) {
        resubs = null
      }
      cb(err, resubs, client)
    }))
}

LevelPersistence.prototype.countOffline = function (cb) {
  var clients = 0
  var subs = 0
  var lastClient = null
  pump(createValueStream(this._db, SUBSCRIPTIONS), through.obj(function (blob, enc, cb) {
    var sub = msgpack.decode(blob)
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
  var that = this
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
  var key = OUTGOING + sub.clientId + ':' + packet.brokerId + ':' + packet.brokerCounter
  this._db.put(key, msgpack.encode(new Packet(packet)), cb)
}

LevelPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
  if (!subs || subs.length === 0) {
    return cb(null, packet)
  }
  var count = 0
  for (var i = 0; i < subs.length; i++) {
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
  var prekey = OUTGOING + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
  var postkey = OUTGOINGID + client.id + ':' + packet.messageId

  that._db.get(prekey, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      cb(new Error('no such packet'), client, packet)
      return
    } else if (err) {
      cb(err, client, packet)
      return
    }

    var decoded = msgpack.decode(blob)

    if (decoded.messageId > 0) {
      that._db.del(OUTGOINGID + client.id + ':' + decoded.messageId)
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
  var postkey = OUTGOINGID + client.id + ':' + packet.messageId
  that._db.get(postkey, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      return cb(new Error('no such packet'))
    } else if (err) {
      return cb(err)
    }

    var decoded = msgpack.decode(blob)

    packet.brokerId = decoded.brokerId
    packet.brokerCounter = decoded.brokerCounter
    cb(null)
  })
}

LevelPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
  var that = this
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
  var that = this
  var key = OUTGOINGID + client.id + ':' + packet.messageId
  this._db.get(key, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      return cb(null)
    } else if (err) {
      return cb(err)
    }

    var packet = msgpack.decode(blob)
    var prekey = 'outgoing:' + client.id + ':' + packet.brokerId + ':' + packet.brokerCounter
    var batch = that._db.batch()
    batch.del(key)
    batch.del(prekey)
    batch.write(function (err) {
      cb(err, packet)
    })
  })
}

LevelPersistence.prototype.outgoingStream = function (client) {
  var key = OUTGOING + client.id
  return pump(createValueStream(this._db, key), through.obj(function (blob, enc, cb) {
    cb(null, msgpack.decode(blob))
  }))
}

LevelPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  var key = INCOMING + client.id + ':' + packet.messageId
  var newp = new Packet(packet)
  newp.messageId = packet.messageId
  this._db.put(key, msgpack.encode(newp), cb)
}

LevelPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  var key = INCOMING + client.id + ':' + packet.messageId
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
  var key = INCOMING + client.id + ':' + packet.messageId
  this._db.del(key, cb)
}

LevelPersistence.prototype.putWill = function (client, packet, cb) {
  var key = WILL + client.id

  packet.brokerId = this.broker.id
  packet.clientId = client.id

  this._db.put(key, msgpack.encode(packet), function (err) {
    cb(err, client)
  })
}

LevelPersistence.prototype.getWill = function (client, cb) {
  var key = WILL + client.id
  this._db.get(key, encodingOption, function (err, blob) {
    if (err && err.notFound) {
      cb(null, null, client)
    } else {
      cb(err, msgpack.decode(blob), client)
    }
  })
}

LevelPersistence.prototype.delWill = function (client, cb) {
  var key = WILL + client.id
  var that = this
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
  var valueStream = createValueStream(this._db, WILL)

  if (!brokers) {
    return pump(valueStream, through.obj(function (blob, enc, cb) {
      cb(null, msgpack.decode(blob))
    }))
  }

  return pump(valueStream, through.obj(function (blob, enc, cb) {
    var chunk = msgpack.decode(blob)
    if (!brokers[chunk.brokerId]) {
      this.push(chunk)
    }
    cb()
  }))
}

LevelPersistence.prototype.getClientList = function (topic) {
  var valueStream = createValueStream(this._db, SUBSCRIPTIONS)

  return pump(valueStream, through.obj(function (blob, enc, cb) {
    var chunk = msgpack.decode(blob)
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
