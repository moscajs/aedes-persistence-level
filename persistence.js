const Packet = require('aedes-packet')
const msgpack = require('msgpack-lite')
const { EventEmitter } = require('node:events')
const { Readable } = require('node:stream')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const { QlobberTrue } = require('qlobber')

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
const LEVEL_NOT_FOUND = 'LEVEL_NOT_FOUND'

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
  const qlobber = new QlobberTrue(QlobberOpts)
  qlobber.add(pattern)

  for await (const packet of decodedDbValues(db, RETAINED)) {
    if (qlobber.test(packet.topic)) {
      yield packet
    }
  }
}

async function subscriptionsByClient (db, client) {
  const resubs = []
  for await (const sub of decodedDbValues(db, subByClientKey(client.id))) {
    // remove clientId from sub
    const { clientId, ...resub } = sub
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

function toSubKey (sub) {
  return `${subByClientKey(sub.clientId)}:${sub.topic}`
}

function callCb (cb, err, value) {
  if (typeof cb === 'function') {
    cb(err, value)
  }
}

class LevelPersistence extends EventEmitter {
  // private class members start with #
  #db
  #trie
  #ready

  constructor (db) {
    super()
    this.#db = db
    this.#trie = new QlobberSub(QlobberOpts)
    this.#ready = false

    loadSubscriptions(this.#db, this.#trie)
      .then(() => {
        this.#ready = true
        this.emit('ready')
      })
      .catch((err) => {
        this.emit('error', err)
      })
  }

  async #dbGet (key, cb) {
    const blob = await this.#db.get(key, encodingOption)
    if (blob !== undefined) {
      cb(undefined, msgpack.decode(blob))
      return
    }
    cb(LEVEL_NOT_FOUND, null)
  }

  async #dbPut (key, value, cb) {
    await this.#db.put(key, msgpack.encode(value), encodingOption)
    callCb(cb)
  }

  async #dbDel (key, cb) {
    await this.#db.del(key)
    callCb(cb)
  }

  async #dbBatch (opArray, cb) {
    await this.#db.batch(opArray, encodingOption)
    callCb(cb)
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

    const opArray = []
    for (const subscription of subs) {
      const sub = Object.assign({}, subscription)
      sub.clientId = client.id
      addSubToTrie(this.#trie, sub)
      const ops = {
        type: 'put',
        key: toSubKey(sub),
        value: msgpack.encode(sub)
      }
      opArray.push(ops)
    }
    this.#dbBatch(opArray, (err) => {
      cb(err, client)
    })
  }

  removeSubscriptions (client, topics, cb) {
    const opArray = []
    for (const topic of topics) {
      const sub = {
        clientId: client.id,
        topic
      }
      delSubFromTrie(this.#trie, sub)
      const ops = {
        type: 'del',
        key: toSubKey(sub)
      }
      opArray.push(ops)
    }
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
    this.subscriptionsByClient(client, (err, subs) => {
      if (err || !subs) return cb(err, client)

      this.removeSubscriptions(
        client,
        subs.map(sub => sub.topic),
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
    if (packet.brokerId) {
      this.#updateWithBrokerData(client, packet, cb)
    } else {
      this.#augmentWithBrokerData(client, packet, (err) => {
        if (err) return cb(err, client, packet)
        this.#updateWithBrokerData(client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const key = outgoingByIdKey(client.id, packet.messageId)
    this.#dbGet(key, (err, packet) => {
      if (err === LEVEL_NOT_FOUND) {
        cb(null)
        return
      }
      if (err) {
        cb(err)
        return
      }

      const prekey = outgoingKey(client.id, packet.brokerId, packet.brokerCounter)
      const batch = this.#db.batch()
      batch.del(key)
      batch.del(prekey)
      batch.write().then(err => {
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
      if (err === LEVEL_NOT_FOUND) {
        cb(new Error('no such packet'), client)
        return
      }
      if (err) {
        cb(err, client)
        return
      }
      cb(null, packet, client)
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
      if (err === LEVEL_NOT_FOUND) {
        cb(null, null, client)
        return
      }
      cb(err, will, client)
    })
  }

  delWill (client, cb) {
    const key = willKey(client.id)
    this.#dbGet(key, (err, will) => {
      if (err) {
        cb(err, null, client)
        return
      }
      this.#dbDel(key, (err) => {
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

    this.#dbGet(prekey, (err, decoded) => {
      if (err === LEVEL_NOT_FOUND) {
        cb(new Error('no such packet'), client, packet)
        return
      }
      if (err) {
        cb(err, client, packet)
        return
      }

      if (decoded.messageId > 0) {
        this.#dbDel(outgoingByIdKey(client.id, decoded.messageId))
      }

      this.#dbPut(postkey, packet, (err) => {
        if (err) {
          cb(err, client, packet)
          return
        }
        this.#dbPut(prekey, packet, (err) => {
          cb(err, client, packet)
        })
      })
    })
  }

  #augmentWithBrokerData (client, packet, cb) {
    const postkey = outgoingByIdKey(client.id, packet.messageId)
    this.#dbGet(postkey, (err, decoded) => {
      if (err === LEVEL_NOT_FOUND) {
        cb(new Error('no such packet'))
        return
      }
      if (err) {
        cb(err)
        return
      }

      packet.brokerId = decoded.brokerId
      packet.brokerCounter = decoded.brokerCounter
      cb(null)
    })
  }

  async destroy (cb) {
    await this.#db.close()
    callCb(cb)
  }
}

function addSubToTrie (trie, sub) {
  let add = false
  const matched = trie.match(sub.topic)
  if (matched.length > 0) {
    add = true
    for (const match of matched) {
      if (match.clientId === sub.clientId) {
        if (match.qos === sub.qos) {
          add = false
          break
        }
        trie.remove(match.topic, match)
        if (sub.qos === 0) {
          add = false
        }
      }
    }
  } else if (sub.qos > 0) {
    add = true
  }
  if (add) {
    trie.add(sub.topic, sub)
  }
}

function delSubFromTrie (trie, sub) {
  const matches = trie.match(sub.topic)
  for (const match of matches) {
    if (sub.clientId === match.clientId && sub.topic === match.topic) {
      trie.remove(sub.topic, sub)
    }
  }
}

module.exports = (db) => {
  return new LevelPersistence(db)
}
module.exports.LevelPersistence = LevelPersistence
