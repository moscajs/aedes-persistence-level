const test = require('node:test')
const assert = require('node:assert/strict')
const persistence = require('../persistence.js')
const abs = require('aedes-persistence/abstract')
const { Level } = require('level') // Level >= 8.0.0

const { randomUUID } = require('node:crypto')
const { tmpdir } = require('node:os')
const { join } = require('node:path')
const { mkdirSync } = require('node:fs')

function tempDir () {
  const dir = join(tmpdir(), 'aedes-persistence-level-test', randomUUID())
  mkdirSync(dir, { recursive: true })
  return dir
}

function leveldb () {
  return new Level(tempDir())
}

abs({
  test,
  persistence () {
    return persistence(leveldb())
  }
})

test('restore', t => {
  const db = leveldb()
  const instance = persistence(db)
  const client = {
    id: 'abcde'
  }

  const subs = [{
    topic: 'hello',
    qos: 1
  }, {
    topic: 'hello/#',
    qos: 1
  }, {
    topic: 'matteo',
    qos: 1
  }]

  instance.addSubscriptions(client, subs, err => {
    assert.ok(!err, 'no error')
    const instance2 = persistence(db)
    instance2.subscriptionsByTopic('hello', (err, resubs) => {
      assert.ok(!err, 'no error')
      assert.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello/#',
        qos: 1,
        rh: undefined,
        rap: undefined,
        nl: undefined
      }, {
        clientId: client.id,
        topic: 'hello',
        qos: 1,
        rh: undefined,
        rap: undefined,
        nl: undefined
      }])
      instance.destroy()
    })
  })
})

test('outgoing update after enqueuing a possible offline message', t => {
  const db = leveldb()
  const instance = persistence(db)
  const client = {
    clientId: 'abcde'
  }

  const client1 = {
    id: 'abcde'
  }

  const packet = {
    cmd: 'publish',
    brokerId: 'adasdasd',
    brokerCounter: 0,
    topic: 'test',
    payload: 'Return of the Jedi',
    messageId: 7
  }

  const updatePacket = {
    cmd: 'pubrel',
    messageId: 7
  }
  // Enqueue an offline packet
  instance.outgoingEnqueue(client, packet, (err, packet1) => {
    assert.ifError(err)
    // When the client comes back online, aedes calls emptyQueue which calls outgoingUpdate
    instance.outgoingUpdate(client1, packet, (err, client, packet) => {
      assert.ok(!err, 'no error')
      // When pubrel is published, outgoingUpdate is called again without the broker Id
      instance.outgoingUpdate(client, updatePacket, (err, client, packet) => {
        assert.ok(!err, 'no error')
        instance.destroy()
      })
    })
  })
})

test('Dont replace subscriptions with different QoS if client id is different', t => {
  const db = leveldb()
  const instance = persistence(db)
  const client = {
    id: 'test'
  }

  const client1 = {
    id: 'test.1'
  }

  const sub1 = [{
    topic: 'test/+/dev/#',
    qos: 2
  }]

  const sub2 = [{
    topic: 'test/television/dev/about',
    qos: 1
  }]

  instance.addSubscriptions(client, sub1, err => {
    assert.ok(!err, 'no error')
    instance.addSubscriptions(client1, sub2, err => {
      assert.ok(!err, 'no error')
      instance.subscriptionsByTopic('test/television/dev/about', (err, resubs) => {
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, [{
          topic: 'test/television/dev/about',
          clientId: 'test.1',
          qos: 1,
          rh: undefined,
          rap: undefined,
          nl: undefined
        }, {
          topic: 'test/+/dev/#',
          clientId: 'test',
          qos: 2,
          rh: undefined,
          rap: undefined,
          nl: undefined
        }])
        instance.destroy()
      })
    })
  })
})

test('Replace subscriptions with different QoS if client id is same', t => {
  const db = leveldb()
  const instance = persistence(db)
  const client = {
    id: 'test'
  }

  const sub1 = [{
    topic: 'test/+/dev/#',
    qos: 2
  }]

  const sub2 = [{
    topic: 'test/television/dev/about',
    qos: 1
  }]

  instance.addSubscriptions(client, sub1, err => {
    assert.ok(!err, 'no error')
    instance.addSubscriptions(client, sub2, err => {
      assert.ok(!err, 'no error')
      instance.subscriptionsByTopic('test/television/dev/about', (err, resubs) => {
        assert.ok(!err, 'no error')
        assert.deepEqual(resubs, [{
          topic: 'test/television/dev/about',
          clientId: 'test',
          qos: 1,
          rh: undefined,
          rap: undefined,
          nl: undefined
        }])
        instance.destroy()
      })
    })
  })
})

// The shared abstract suite only cleans between ids that share no prefix
// ('abcde' / 'fghij'), so it never exercises the key-range isolation this
// store's cleanIncoming depends on.
test('cleanIncoming leaves clients with a colliding id prefix alone', async () => {
  const instance = persistence(leveldb())
  const packet = {
    cmd: 'publish',
    topic: 'hello',
    payload: Buffer.from('world'),
    qos: 2,
    dup: false,
    length: 14,
    retain: false,
    messageId: 42
  }
  const target = { id: 'abc' }
  // 'abcde' shares the prefix; the other two probe the ':' delimiter, which
  // only stays unambiguous because encodeURIComponent escapes it
  const survivors = [{ id: 'abcde' }, { id: 'abc:x' }, { id: 'abc%3Ax' }]

  for (const client of [target, ...survivors]) {
    await instance.incomingStorePacket(client, packet)
  }
  await instance.cleanIncoming(target)

  await assert.rejects(
    instance.incomingGetPacket(target, { messageId: packet.messageId }),
    'target packet was removed'
  )
  for (const client of survivors) {
    const stored = await instance.incomingGetPacket(client, { messageId: packet.messageId })
    assert.equal(stored.messageId, packet.messageId, `${client.id} packet survived`)
  }
  await instance.destroy()
})

// Same key-range isolation, for the two per-client prefixes that are read back
// rather than cleared. Without the ':' these scans hand one client another
// client's state.
test('subscriptionsByClient does not return a prefix-colliding client subs', async () => {
  const instance = persistence(leveldb())
  const target = { id: 'abc' }
  const other = { id: 'abcde' }

  await instance.addSubscriptions(target, [{ topic: 'own/topic', qos: 1, rh: 0, rap: true, nl: false }])
  await instance.addSubscriptions(other, [{ topic: 'other/topic', qos: 1, rh: 0, rap: true, nl: false }])

  const subs = await instance.subscriptionsByClient(target)
  assert.deepEqual(subs.map(s => s.topic), ['own/topic'])

  await instance.destroy()
})

test('outgoingStream does not stream a prefix-colliding client queue', async () => {
  const instance = persistence(leveldb())
  const target = { id: 'abc' }
  const other = { id: 'abcde' }
  const mkPacket = (topic, brokerCounter) => ({
    cmd: 'publish',
    topic,
    payload: Buffer.from('world'),
    qos: 1,
    dup: false,
    length: 14,
    retain: false,
    brokerId: 'broker-1',
    brokerCounter
  })

  await instance.outgoingEnqueue({ clientId: target.id, topic: 'own/topic', qos: 1 }, mkPacket('own/topic', 1))
  await instance.outgoingEnqueue({ clientId: other.id, topic: 'other/topic', qos: 1 }, mkPacket('other/topic', 2))

  const topics = []
  for await (const packet of instance.outgoingStream(target)) {
    topics.push(packet.topic)
  }
  assert.deepEqual(topics, ['own/topic'])

  await instance.destroy()
})

// MQTT topics are UTF-8, and they are stored raw after the key prefix. A range
// bounded by '\xff' (bytes C3 BF) stops short of any topic starting above
// U+00FF, which silently hides those rows from every per-prefix scan.
const NON_ASCII_TOPICS = ['ascii/a', 'é/x', 'ÿ/x', 'Ā/x', 'тема', '主题', '😀/x']

test('subscriptions on non-ascii topics are readable and removable', async () => {
  const dir = tempDir()
  let instance = persistence(new Level(dir))
  await instance.setup({ id: 'broker-1' })
  const client = { id: 'abc' }
  const subs = NON_ASCII_TOPICS.map(topic => ({ topic, qos: 1, rh: 0, rap: true, nl: false }))

  await instance.addSubscriptions(client, subs)
  const stored = await instance.subscriptionsByClient(client)
  assert.deepEqual(stored.map(s => s.topic).sort(), [...NON_ASCII_TOPICS].sort())

  // a row the per-client scan cannot see is also a row cleanSubscriptions
  // cannot delete: it would survive teardown and come back via loadSubscriptions
  await instance.cleanSubscriptions(client)
  assert.deepEqual(await instance.subscriptionsByClient(client), [])
  await instance.destroy()

  instance = persistence(new Level(dir))
  await instance.setup({ id: 'broker-1' })
  assert.deepEqual(await instance.subscriptionsByTopic('тема'), [], 'no subscription revived')
  await instance.destroy()
})

test('retained messages on non-ascii topics are streamed', async () => {
  const instance = persistence(leveldb())
  const mkRetained = topic => ({
    cmd: 'publish',
    topic,
    payload: Buffer.from('world'),
    qos: 0,
    dup: false,
    length: 14,
    retain: true
  })

  for (const topic of NON_ASCII_TOPICS) {
    await instance.storeRetained(mkRetained(topic))
  }

  const streamed = []
  for await (const packet of instance.createRetainedStream('#')) {
    streamed.push(packet.topic)
  }
  assert.deepEqual(streamed.sort(), [...NON_ASCII_TOPICS].sort())

  await instance.destroy()
})
