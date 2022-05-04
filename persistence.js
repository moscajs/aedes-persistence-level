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
  valueEncoding: 'buffer'
}

async function * decodedDbValues (db, start) {
  const opts = Object.assign({
    gt: start,
    lt: `${start}\xff`
  }, encodingOption)
  for await (const blob of db.values(opts)) {
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
  for await (const sub of decodedDbValues(db, subByClientKey(client.id))) {
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

// Number.MAX_SAFE_INTEGER has a length of 16 digits
// by padding the ID we ensure correct sorting in the queue
function padId (id) {
  return id?.toString().padStart(16, '0')
}

function outgoingKey (clientId, brokerId, brokerCounter) {
  return `${OUTGOING}${encodeURIComponent(clientId)}:${brokerId}:${padId(brokerCounter)}`
}

function outgoingByClientKey (clientId) {
  return `${OUTGOING}${encodeURIComponent(clientId)}`
}

function outgoingByIdKey (clientId, messageId) {
  return `${OUTGOINGID}${encodeURIComponent(clientId)}:${padId(messageId)}`
}

function incomingKey (clientId, messageId) {
  return `${INCOMING}${encodeURIComponent(clientId)}:${padId(messageId)}`
}

function willKey (clientId) {
  return `${WILL}${encodeURIComponent(clientId)}`
}

function subByClientKey (clientId) {
  return `${SUBSCRIPTIONS}${encodeURIComponent(clientId)}`
}

class LevelPersistence extends EventEmitter {
  // private class members start with #
  #db
  #trie
  #ready
  #keyPadLength

  constructor (db) {
    super()
    this.#db = db
    this.#trie = new Qlobber(QlobberOpts)
    this.#ready = false

    this.#keyPadLength = 16
    const that = this

    loadSubscriptions(this.#db, this.#trie)
      .then(() => {
        that.#ready = true
        that.emit('ready')
      })
      .catch((err) => {
        that.emit('error', err)
      })
  }

  #dbGet (key, cb) {
    this.#db.get(key, encodingOption, (err, blob) => {
      cb(err, (!err) ? msgpack.decode(blob) : null)
    })
  }

  #dbPut (key, value, cb) {
    this.#db.put(key, msgpack.encode(value), encodingOption, cb)
  }

  #dbDel (key, cb) {
    this.#db.del(key, cb)
  }

  #dbBatch (opArray, cb) {
    this.#db.batch(opArray, encodingOption, cb)
  }

  storeRetained (packet, cb) {
    const key = `${RETAINED}${packet.topic}`
    if (packet.payload.length === 0) {
      this.#dbDel(key, cb)
    } else {
      this.#dbPut(key, packet, cb)
    }
  }

  createRetainedStreamCombi (patterns) {
    const iterables = patterns.map((p) => {
      return retainedMessagesByPattern(this.#db, p)
    })
    return Readable.from(multiIterables(iterables))
  }

  createRetainedStream (pattern) {
    return Readable.from(retainedMessagesByPattern(this.#db, pattern))
  }

  addSubscriptions (client, subs, cb) {
    if (!this.#ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    subs = subs
      .map(withClientId, client)
      .map(addSubToTrie, this.#trie)
    const opArray = []
    subs.forEach((sub) => {
      const ops = {}
      ops.type = 'put'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this.#dbBatch(opArray, (err) => {
      cb(err, client)
    })
  }

  removeSubscriptions (client, subs, cb) {
    subs = subs
      .map(toSubObj, client)
      .map(delSubFromTrie, this.#trie)
    const opArray = []
    subs.forEach((sub) => {
      const ops = {}
      ops.type = 'del'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this.#dbBatch(opArray, (err) => {
      cb(err, client)
    })
  }

  subscriptionsByClient (client, cb) {
    subscriptionsByClient(this.#db, client)
      .then((resubs) => cb(null, resubs, client))
      .catch((err) => cb(err, null, null))
  }

  countOffline (cb) {
    countOfflineClients(this.#db)
      .then((res) => cb(null, res.subs, res.clients))
      .catch((err) => cb(err))
  }

  subscriptionsByTopic (pattern, cb) {
    if (!this.#ready) {
      this.once('ready', this.subscriptionsByTopic.bind(this, pattern, cb))
      return this
    }
    cb(null, this.#trie.match(pattern))
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
    const key = outgoingKey(sub.clientId, packet.brokerId, packet.brokerCounter)
    this.#dbPut(key, new Packet(packet), cb)
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
      that.#updateWithBrokerData(client, packet, cb)
    } else {
      this.#augmentWithBrokerData(client, packet, (err) => {
        if (err) return cb(err, client, packet)
        that.#updateWithBrokerData(client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const that = this
    const key = outgoingByIdKey(client.id, packet.messageId)
    this.#dbGet(key, (err, packet) => {
      if (err?.notFound) {
        return cb(null)
      } else if (err) {
        return cb(err)
      }

      const prekey = outgoingKey(client.id, packet.brokerId, packet.brokerCounter)
      const batch = that.#db.batch()
      batch.del(key)
      batch.del(prekey)
      batch.write((err) => {
        cb(err, packet)
      })
    })
  }

  outgoingStream (client) {
    return Readable.from(decodedDbValues(
      this.#db,
      outgoingByClientKey(client.id)
    ))
  }

  incomingStorePacket (client, packet, cb) {
    const key = incomingKey(client.id, packet.messageId)
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    this.#dbPut(key, newp, cb)
  }

  incomingGetPacket (client, packet, cb) {
    const key = incomingKey(client.id, packet.messageId)
    this.#dbGet(key, (err, packet) => {
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
    const key = incomingKey(client.id, packet.messageId)
    this.#dbDel(key, cb)
  }

  putWill (client, packet, cb) {
    const key = willKey(client.id)

    packet.brokerId = this.broker.id
    packet.clientId = client.id

    this.#dbPut(key, packet, (err) => {
      cb(err, client)
    })
  }

  getWill (client, cb) {
    const key = willKey(client.id)
    this.#dbGet(key, (err, will) => {
      if (err && err.notFound) {
        cb(null, null, client)
      } else {
        cb(err, will, client)
      }
    })
  }

  delWill (client, cb) {
    const key = willKey(client.id)
    const that = this
    this.#dbGet(key, (err, will) => {
      if (err) {
        return cb(err, null, client)
      }
      that.#dbDel(key, (err) => {
        cb(err, will, client)
      })
    })
  }

  streamWill (brokers) {
    return Readable.from(willsByBrokers(this.#db, brokers))
  }

  getClientList (topic) {
    return Readable.from(clientListbyTopic(this.#db, topic))
  }

  #updateWithBrokerData (client, packet, cb) {
    const prekey = outgoingKey(client.id, packet.brokerId, packet.brokerCounter)
    const postkey = outgoingByIdKey(client.id, packet.messageId)
    const that = this

    this.#dbGet(prekey, (err, decoded) => {
      if (err && err.notFound) {
        cb(new Error('no such packet'), client, packet)
        return
      } else if (err) {
        cb(err, client, packet)
        return
      }

      if (decoded.messageId > 0) {
        that.#dbDel(outgoingByIdKey(client.id, decoded.messageId))
      }

      that.#dbPut(postkey, packet, (err) => {
        if (err) {
          cb(err, client, packet)
        } else {
          that.#dbPut(prekey, packet, (err) => {
            cb(err, client, packet)
          })
        }
      })
    })
  }

  #augmentWithBrokerData (client, packet, cb) {
    const postkey = outgoingByIdKey(client.id, packet.messageId)
    this.#dbGet(postkey, (err, decoded) => {
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
    this.#db.close(cb)
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
  return `${subByClientKey(sub.clientId)}:${sub.topic}`
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
