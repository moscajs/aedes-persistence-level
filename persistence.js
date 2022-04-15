const Qlobber = require('qlobber').Qlobber
const Packet = require('aedes-packet')
const msgpack = require('msgpack-lite')
const EventEmitter = require('events').EventEmitter
const { Readable } = require('stream')

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
// in level 8.0.0 'binary' is an alias for 'buffer'
const encodingOption = {
  valueEncoding: 'binary'
}

function dbValues (db, start) {
  const opts = Object.assign({
    gt: start,
    lt: `${start}\xff`
  }, encodingOption)
  // Level 8
  if (typeof db.values === 'function') {
    return db.values(opts)
  }
  // level 7
  if (Symbol.iterator in db.iterator) {
    return dbValuesLevel7(db, start, opts)
  }
  return dbValuesLevel6(db, start, opts)
}

async function * dbValuesLevel7 (db, start, opts) {
  for await (const [, value] of db.iterator(opts)) {
    yield value
  }
}

function nextLevel6 (iter) {
  return new Promise((resolve, reject) => {
    iter.next((err, key, value) => {
      if (err) {
        reject(err)
        return
      }
      resolve({ key, value })
    })
  })
}

function endLevel6 (iter) {
  return new Promise((resolve, reject) => {
    iter.end(resolve, reject)
  })
}

async function * dbValuesLevel6 (db, start, opts) {
  const iter = db.iterator(opts)
  let hasValue = true
  while (hasValue) {
    const item = await nextLevel6(iter)
    if (item?.value) {
      yield item.value
    } else {
      await endLevel6(iter)
      hasValue = false
    }
  }
}

async function * decodedDbValues (db, start) {
  for await (const blob of dbValues(db, start)) {
    yield msgpack.decode(blob)
  }
}

async function * multiIterables (iterables) {
  for (const iter of iterables) {
    yield * iter
  }
}

async function loadSubscriptions (db, trie) {
  for await (const chunk of decodedDbValues(db, SUBSCRIPTIONS)) {
    trie.add(chunk.topic, chunk)
  }
}

async function * retainedMessagesByPattern (db, pattern) {
  const qlobber = new Qlobber(QlobberOpts)
  qlobber.add(pattern, true)

  for await (const packet of decodedDbValues(db, RETAINED)) {
    if (qlobber.match(packet.topic).length) {
      yield packet
    }
  }
}

async function subscriptionsByClient (db, client) {
  const resubs = []
  for await (const sub of decodedDbValues(db, `${SUBSCRIPTIONS}${client.id}`)) {
    const resub = rmClientId(sub)
    resubs.push(resub)
  }
  return ((resubs.length > 0) ? resubs : null)
}

async function countOfflineClients (db) {
  let clients = 0
  let subs = 0
  let lastClient = null

  for await (const sub of decodedDbValues(db, SUBSCRIPTIONS)) {
    if (lastClient !== sub.clientId) {
      lastClient = sub.clientId
      clients++
    }
    if (sub.qos > 0) {
      subs++
    }
  }
  return { subs, clients }
}

function outgoingByClient (db, client) {
  const key = `${OUTGOING}${client.id}`
  return decodedDbValues(db, key)
}

async function * willsByBrokers (db, brokers) {
  for await (const chunk of decodedDbValues(db, WILL)) {
    if (!brokers) {
      yield chunk
    } else {
      if (!brokers[chunk.brokerId]) {
        yield chunk
      }
    }
  }
}

async function * clientListbyTopic (db, topic) {
  for await (const chunk of decodedDbValues(db, SUBSCRIPTIONS)) {
    if (chunk.topic === topic) {
      yield chunk.clientId
    }
  }
}

class LevelPersistence extends EventEmitter {
  constructor (db) {
    super()
    this._db = db
    this._trie = new Qlobber(QlobberOpts)
    this._ready = false
    const that = this

    loadSubscriptions(this._db, this._trie)
      .then(() => {
        that._ready = true
        that.emit('ready')
      })
      .catch((err) => {
        that.emit('error', err)
      })
  }

  // private methods start with _
  _dbGet (key, cb) {
    this._db.get(key, encodingOption, (err, blob) => {
      cb(err, (!err) ? msgpack.decode(blob) : null)
    })
  }

  _dbPut (key, value, cb) {
    this._db.put(key, msgpack.encode(value), encodingOption, cb)
  }

  _dbDel (key, cb) {
    this._db.del(key, cb)
  }

  _dbBatch (opArray, cb) {
    this._db.batch(opArray, encodingOption, cb)
  }

  storeRetained (packet, cb) {
    const key = `${RETAINED}${packet.topic}`
    if (packet.payload.length === 0) {
      this._dbDel(key, cb)
    } else {
      this._dbPut(key, packet, cb)
    }
  }

  createRetainedStreamCombi (patterns) {
    const iterables = patterns.map((p) => {
      return retainedMessagesByPattern(this._db, p)
    })
    return Readable.from(multiIterables(iterables))
  }

  createRetainedStream (pattern) {
    return Readable.from(retainedMessagesByPattern(this._db, pattern))
  }

  addSubscriptions (client, subs, cb) {
    if (!this._ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    subs = subs
      .map(withClientId, client)
      .map(addSubToTrie, this._trie)
    const opArray = []
    subs.forEach((sub) => {
      const ops = {}
      ops.type = 'put'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this._dbBatch(opArray, (err) => {
      cb(err, client)
    })
  }

  removeSubscriptions (client, subs, cb) {
    subs = subs
      .map(toSubObj, client)
      .map(delSubFromTrie, this._trie)
    const opArray = []
    subs.forEach((sub) => {
      const ops = {}
      ops.type = 'del'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this._dbBatch(opArray, (err) => {
      cb(err, client)
    })
  }

  subscriptionsByClient (client, cb) {
    subscriptionsByClient(this._db, client)
      .then((resubs) => cb(null, resubs, client))
      .catch((err) => cb(err, null, null))
  }

  countOffline (cb) {
    countOfflineClients(this._db)
      .then((res) => cb(null, res.subs, res.clients))
      .catch((err) => cb(err))
  }

  subscriptionsByTopic (pattern, cb) {
    if (!this._ready) {
      this.once('ready', this.subscriptionsByTopic.bind(this, pattern, cb))
      return this
    }
    cb(null, this._trie.match(pattern))
  }

  cleanSubscriptions (client, cb) {
    const that = this
    this.subscriptionsByClient(client, (err, subs) => {
      if (err || !subs) return cb(err, client)

      that.removeSubscriptions(
        client,
        subs.map((sub) => {
          return sub.topic
        }),
        (err) => {
          cb(err, client)
        }
      )
    })
  }

  outgoingEnqueue (sub, packet, cb) {
    const key =
      `${OUTGOING}${sub.clientId}:${packet.brokerId}:${packet.brokerCounter}`
    this._dbPut(key, new Packet(packet), cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!subs || subs.length === 0) {
      return cb(null, packet)
    }
    let count = 0
    for (let i = 0; i < subs.length; i++) {
      this.outgoingEnqueue(subs[i], packet, (err) => {
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

  outgoingUpdate (client, packet, cb) {
    const that = this
    if (packet.brokerId) {
      that._updateWithBrokerData(client, packet, cb)
    } else {
      this._augmentWithBrokerData(client, packet, (err) => {
        if (err) return cb(err, client, packet)
        that._updateWithBrokerData(client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const that = this
    const key = `${OUTGOINGID}${client.id}:${packet.messageId}`
    this._dbGet(key, (err, packet) => {
      if (err?.notFound) {
        return cb(null)
      } else if (err) {
        return cb(err)
      }

      const prekey =
        `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
      const batch = that._db.batch()
      batch.del(key)
      batch.del(prekey)
      batch.write((err) => {
        cb(err, packet)
      })
    })
  }

  outgoingStream (client) {
    return Readable.from(outgoingByClient(this._db, client))
  }

  incomingStorePacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    this._dbPut(key, newp, cb)
  }

  incomingGetPacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    this._dbGet(key, (err, packet) => {
      if (err && err.notFound) {
        cb(new Error('no such packet'), client)
      } else if (err) {
        cb(err, client)
      } else {
        cb(null, packet, client)
      }
    })
  }

  incomingDelPacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    this._dbDel(key, cb)
  }

  putWill (client, packet, cb) {
    const key = `${WILL}${client.id}`

    packet.brokerId = this.broker.id
    packet.clientId = client.id

    this._dbPut(key, packet, (err) => {
      cb(err, client)
    })
  }

  getWill (client, cb) {
    const key = `${WILL}${client.id}`
    this._dbGet(key, (err, will) => {
      if (err && err.notFound) {
        cb(null, null, client)
      } else {
        cb(err, will, client)
      }
    })
  }

  delWill (client, cb) {
    const key = `${WILL}${client.id}`
    const that = this
    this._dbGet(key, (err, will) => {
      if (err) {
        return cb(err, null, client)
      }
      that._dbDel(key, (err) => {
        cb(err, will, client)
      })
    })
  }

  streamWill (brokers) {
    return Readable.from(willsByBrokers(this._db, brokers))
  }

  getClientList (topic) {
    return Readable.from(clientListbyTopic(this._db, topic))
  }

  _updateWithBrokerData (client, packet, cb) {
    const prekey =
      `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
    const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`
    const that = this

    this._dbGet(prekey, (err, decoded) => {
      if (err && err.notFound) {
        cb(new Error('no such packet'), client, packet)
        return
      } else if (err) {
        cb(err, client, packet)
        return
      }

      if (decoded.messageId > 0) {
        that._dbDel(`${OUTGOINGID}${client.id}:${decoded.messageId}`)
      }

      that._dbPut(postkey, packet, (err) => {
        if (err) {
          cb(err, client, packet)
        } else {
          that._dbPut(prekey, packet, (err) => {
            cb(err, client, packet)
          })
        }
      })
    })
  }

  _augmentWithBrokerData (client, packet, cb) {
    const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`
    this._dbGet(postkey, (err, decoded) => {
      if (err && err.notFound) {
        return cb(new Error('no such packet'))
      } else if (err) {
        return cb(err)
      }

      packet.brokerId = decoded.brokerId
      packet.brokerCounter = decoded.brokerCounter
      cb(null)
    })
  }

  destroy (cb) {
    this._db.close(cb)
  }
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

function toSubObj (topic) {
  return {
    clientId: this.id,
    topic
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

function rmClientId (sub) {
  return {
    topic: sub.topic,
    qos: sub.qos
  }
}

module.exports = (db) => {
  return new LevelPersistence(db)
}
module.exports.LevelPersistence = LevelPersistence
