const Qlobber = require('qlobber').Qlobber
const Packet = require('aedes-packet')
const through = require('through2')
const msgpack = require('msgpack-lite')
const callbackStream = require('callback-stream')
const pump = require('pump')
const EventEmitter = require('events').EventEmitter
const multistream = require('multistream')
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
  console.log('level7', typeof db.iterator)
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
  console.log('level6', typeof db.iterator)

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

function createValueStream (db, start) {
  return Readable.from(dbValues(db, start))
}

async function loadSubscriptions (db, trie) {
  try {
    for await (const blob of dbValues(db, SUBSCRIPTIONS)) {
      const chunk = msgpack.decode(blob)
      trie.add(chunk.topic, chunk)
    }
  } catch (error) {
    console.log(error)
  }
}

class LevelPersistence extends EventEmitter {
  constructor (db) {
    super()
    this._db = db
    this._trie = new Qlobber(QlobberOpts)
    this._ready = false

    const trie = this._trie
    const that = this

    loadSubscriptions(this._db, trie)
      .then(() => {
        that._ready = true
        that.emit('ready')
      })
      .catch(err => {
        that.emit('error', err)
      })
  }

  storeRetained (packet, cb) {
    const key = `${RETAINED}${packet.topic}`
    if (packet.payload.length === 0) {
      this._db.del(key, cb)
    } else {
      this._db.put(key, msgpack.encode(packet), encodingOption, cb)
    }
  }

  createRetainedStreamCombi (patterns) {
    const that = this
    const streams = patterns.map(p => {
      return that.createRetainedStream(p)
    })
    return multistream.obj(streams)
  }

  createRetainedStream (pattern) {
    const qlobber = new Qlobber(QlobberOpts)
    qlobber.add(pattern, true)

    const res = through.obj((blob, encoding, deliver) => {
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

  addSubscriptions (client, subs, cb) {
    if (!this._ready) {
      this.once('ready', this.addSubscriptions.bind(this, client, subs, cb))
      return
    }

    subs = subs
      .map(withClientId, client)
      .map(addSubToTrie, this._trie)
    const opArray = []
    subs.forEach(sub => {
      const ops = {}
      ops.type = 'put'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this._db.batch(opArray, encodingOption, err => {
      cb(err, client)
    })
  }

  removeSubscriptions (client, subs, cb) {
    subs = subs
      .map(toSubObj, client)
      .map(delSubFromTrie, this._trie)
    const opArray = []
    subs.forEach(sub => {
      const ops = {}
      ops.type = 'del'
      ops.key = toSubKey(sub)
      ops.value = msgpack.encode(sub)
      opArray.push(ops)
    })
    this._db.batch(opArray, encodingOption, err => {
      cb(err, client)
    })
  }

  subscriptionsByClient (client, cb) {
    createValueStream(this._db, `${SUBSCRIPTIONS}${client.id}`)
      .pipe(callbackStream({ objectMode: true }, (err, blob) => {
        const subs = blob.map(msgpack.decode)
        let resubs = subs.map(rmClientId)
        if (resubs.length === 0) {
          resubs = null
        }
        cb(err, resubs, client)
      }))
  }

  countOffline (cb) {
    let clients = 0
    let subs = 0
    let lastClient = null
    pump(
      createValueStream(this._db, SUBSCRIPTIONS),
      through.obj((blob, enc, cb) => {
        const sub = msgpack.decode(blob)
        if (lastClient !== sub.clientId) {
          lastClient = sub.clientId
          clients++
        }
        if (sub.qos > 0) {
          subs++
        }
        cb()
      }),
      err => {
        cb(err, subs, clients)
      }
    )
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
        subs.map(sub => {
          return sub.topic
        }),
        err => {
          cb(err, client)
        }
      )
    })
  }

  outgoingEnqueue (sub, packet, cb) {
    const key =
      `${OUTGOING}${sub.clientId}:${packet.brokerId}:${packet.brokerCounter}`
    this._db.put(key, msgpack.encode(new Packet(packet)), encodingOption, cb)
  }

  outgoingEnqueueCombi (subs, packet, cb) {
    if (!subs || subs.length === 0) {
      return cb(null, packet)
    }
    let count = 0
    for (let i = 0; i < subs.length; i++) {
      this.outgoingEnqueue(subs[i], packet, err => {
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
      updateWithBrokerData(this, client, packet, cb)
    } else {
      augmentWithBrokerData(this, client, packet, err => {
        if (err) return cb(err, client, packet)

        updateWithBrokerData(that, client, packet, cb)
      })
    }
  }

  outgoingClearMessageId (client, packet, cb) {
    const that = this
    const key = `${OUTGOINGID}${client.id}:${packet.messageId}`
    this._db.get(key, encodingOption, (err, blob) => {
      if (err && err.notFound) {
        return cb(null)
      } else if (err) {
        return cb(err)
      }

      const packet = msgpack.decode(blob)
      const prekey =
        `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
      const batch = that._db.batch()
      batch.del(key)
      batch.del(prekey)
      batch.write(err => {
        cb(err, packet)
      })
    })
  }

  outgoingStream (client) {
    const key = `${OUTGOING}${client.id}`
    return pump(
      createValueStream(this._db, key),
      through.obj((blob, enc, cb) => {
        cb(null, msgpack.decode(blob))
      })
    )
  }

  incomingStorePacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    const newp = new Packet(packet)
    newp.messageId = packet.messageId
    this._db.put(key, msgpack.encode(newp), encodingOption, cb)
  }

  incomingGetPacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    this._db.get(key, encodingOption, (err, blob) => {
      if (err && err.notFound) {
        cb(new Error('no such packet'), client)
      } else if (err) {
        cb(err, client)
      } else {
        cb(null, msgpack.decode(blob), client)
      }
    })
  }

  incomingDelPacket (client, packet, cb) {
    const key = `${INCOMING}${client.id}:${packet.messageId}`
    this._db.del(key, cb)
  }

  putWill (client, packet, cb) {
    const key = `${WILL}${client.id}`

    packet.brokerId = this.broker.id
    packet.clientId = client.id

    this._db.put(key, msgpack.encode(packet), encodingOption, err => {
      cb(err, client)
    })
  }

  getWill (client, cb) {
    const key = `${WILL}${client.id}`
    this._db.get(key, encodingOption, (err, blob) => {
      if (err && err.notFound) {
        cb(null, null, client)
      } else {
        cb(err, msgpack.decode(blob), client)
      }
    })
  }

  delWill (client, cb) {
    const key = `${WILL}${client.id}`
    const that = this
    this._db.get(key, encodingOption, (err, blob) => {
      if (err) {
        return cb(err, null, client)
      }
      that._db.del(key, err => {
        cb(err, msgpack.decode(blob), client)
      })
    })
  }

  streamWill (brokers) {
    const valueStream = createValueStream(this._db, WILL)

    if (!brokers) {
      return pump(
        valueStream,
        through.obj((blob, enc, cb) => {
          cb(null, msgpack.decode(blob))
        })
      )
    }

    return pump(
      valueStream,
      through.obj(function (blob, enc, cb) {
        const chunk = msgpack.decode(blob)
        if (!brokers[chunk.brokerId]) {
          this.push(chunk)
        }
        cb()
      })
    )
  }

  getClientList (topic) {
    const valueStream = createValueStream(this._db, SUBSCRIPTIONS)

    return pump(
      valueStream,
      through.obj(function (blob, enc, cb) {
        const chunk = msgpack.decode(blob)
        if (chunk.topic === topic) {
          this.push(chunk.clientId)
        }
        cb()
      })
    )
  }

  destroy (cb) {
    this._db.close(cb)
  }
}

// inherits(LevelPersistence, EventEmitter)

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

function updateWithBrokerData (that, client, packet, cb) {
  const prekey =
    `${OUTGOING}${client.id}:${packet.brokerId}:${packet.brokerCounter}`
  const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`

  that._db.get(prekey, encodingOption, (err, blob) => {
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

    that._db.put(postkey, msgpack.encode(packet), encodingOption, err => {
      if (err) {
        cb(err, client, packet)
      } else {
        that._db.put(prekey, msgpack.encode(packet), encodingOption, err => {
          cb(err, client, packet)
        })
      }
    })
  })
}

function augmentWithBrokerData (that, client, packet, cb) {
  const postkey = `${OUTGOINGID}${client.id}:${packet.messageId}`
  that._db.get(postkey, encodingOption, (err, blob) => {
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

module.exports = db => { return new LevelPersistence(db) }
module.exports.LevelPersistence = LevelPersistence
