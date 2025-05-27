const Packet = require('aedes-packet')
const msgpack = require('msgpack-lite')
const { Readable } = require('node:stream')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const { QlobberTrue } = require('qlobber')

const QLOBBER_OPTIONS = {
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
  const qlobber = new QlobberTrue(QLOBBER_OPTIONS)
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
  return (resubs)
}

async function countOfflineClients (db) {
  let clientsCount = 0
  let subsCount = 0
  let lastClient = null

  for await (const sub of decodedDbValues(db, SUBSCRIPTIONS)) {
    if (lastClient !== sub.clientId) {
      lastClient = sub.clientId
      clientsCount++
    }
    if (sub.qos > 0) {
      subsCount++
    }
  }
  return { subsCount, clientsCount }
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

class AsyncLevelPersistence {
  // private class members start with #
  #db
  #trie

  constructor (db) {
    this.#db = db
    this.#trie = new QlobberSub(QLOBBER_OPTIONS)
  }

  async setup (broker) {
    this.broker = broker
    await loadSubscriptions(this.#db, this.#trie)
  }

  async #dbGet (key) {
    const blob = await this.#db.get(key, encodingOption)
    if (blob !== undefined) {
      return msgpack.decode(blob)
    }
    return LEVEL_NOT_FOUND
  }

  async #dbPut (key, value) {
    await this.#db.put(key, msgpack.encode(value), encodingOption)
  }

  async #dbDel (key) {
    await this.#db.del(key)
  }

  async #dbBatch (opArray) {
    await this.#db.batch(opArray, encodingOption)
  }

  async storeRetained (packet) {
    const key = `${RETAINED}${packet.topic}`
    if (packet.payload.length === 0) {
      await this.#dbDel(key)
    } else {
      await this.#dbPut(key, packet)
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

  async addSubscriptions (client, subs) {
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
    await this.#dbBatch(opArray)
  }

  async removeSubscriptions (client, topics) {
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
    await this.#dbBatch(opArray)
  }

  async subscriptionsByClient (client) {
    return await subscriptionsByClient(this.#db, client)
  }

  async countOffline () {
    return await countOfflineClients(this.#db)
  }

  async subscriptionsByTopic (pattern) {
    const subs = this.#trie.match(pattern)
    return subs
  }

  async cleanSubscriptions (client) {
    const subs = await this.subscriptionsByClient(client)
    if (subs.length > 0) {
      const remSubs = subs.map(sub => sub.topic)
      await this.removeSubscriptions(client, remSubs)
    }
  }

  async outgoingEnqueue (sub, packet) {
    const key = outgoingKey(sub.clientId, packet.brokerId, packet.brokerCounter)
    await this.#dbPut(key, new Packet(packet))
  }

  async outgoingEnqueueCombi (subs, packet) {
    if (!subs || subs.length === 0) {
      return packet
    }

    const promises = []
    for (let i = 0; i < subs.length; i++) {
      promises.push(this.outgoingEnqueue(subs[i], packet))
    }
    await Promise.all(promises)
  }

  async outgoingUpdate (client, packet) {
    if (packet.brokerId) {
      await this.#updateWithBrokerData(client, packet)
    } else {
      const updated = await this.#augmentWithBrokerData(client, packet)
      await this.#updateWithBrokerData(client, updated)
    }
  }

  async outgoingClearMessageId (client, packet) {
    const key = outgoingByIdKey(client.id, packet.messageId)
    const resPacket = await this.#dbGet(key)
    if (resPacket === LEVEL_NOT_FOUND) {
      return
    }
    const prekey = outgoingKey(client.id, resPacket.brokerId, resPacket.brokerCounter)
    const batch = this.#db.batch()
    batch.del(key)
    batch.del(prekey)
    await batch.write()
    return resPacket
  }

  outgoingStream (client) {
    return Readable.from(decodedDbValues(
      this.#db,
      outgoingByClientKey(client.id)
    ))
  }

  async incomingStorePacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    await this.#dbPut(key, newp)
  }

  async incomingGetPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    const decoded = await this.#dbGet(key)
    if (decoded === LEVEL_NOT_FOUND) {
      throw new Error('no such packet')
    }
    return decoded
  }

  async incomingDelPacket (client, packet) {
    const key = incomingKey(client.id, packet.messageId)
    await this.#dbDel(key)
  }

  async putWill (client, packet) {
    const key = willKey(client.id)
    packet.brokerId = this.broker?.id
    packet.clientId = client.id
    await this.#dbPut(key, packet)
  }

  async getWill (client) {
    const key = willKey(client.id)
    const will = await this.#dbGet(key)
    if (will === LEVEL_NOT_FOUND) {
      return
    }
    return will
  }

  async delWill (client) {
    const key = willKey(client.id)
    const will = await this.#dbGet(key)
    if (will === LEVEL_NOT_FOUND) {
      throw new Error('Will not found')
    }
    await this.#dbDel(key)
    return will
  }

  streamWill (brokers) {
    return Readable.from(willsByBrokers(this.#db, brokers))
  }

  getClientList (topic) {
    return Readable.from(clientListbyTopic(this.#db, topic))
  }

  async #updateWithBrokerData (client, packet) {
    const prekey = outgoingKey(client.id, packet.brokerId, packet.brokerCounter)
    const postkey = outgoingByIdKey(client.id, packet.messageId)

    const decoded = await this.#dbGet(prekey)
    if (decoded === LEVEL_NOT_FOUND) {
      throw new Error('no such packet')
    }

    if (decoded.messageId > 0) {
      await this.#dbDel(outgoingByIdKey(client.id, decoded.messageId))
    }
    await this.#dbPut(postkey, packet)
    await this.#dbPut(prekey, packet)
  }

  async #augmentWithBrokerData (client, packet) {
    const postkey = outgoingByIdKey(client.id, packet.messageId)
    const decoded = await this.#dbGet(postkey)
    if (decoded === LEVEL_NOT_FOUND) {
      throw new Error('no such packet')
    }
    packet.brokerId = decoded.brokerId
    packet.brokerCounter = decoded.brokerCounter
    return packet
  }

  async destroy () {
    await this.#db.close()
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

module.exports = AsyncLevelPersistence
