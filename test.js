const test = require('tape').test
const persistence = require('./')
const abs = require('aedes-persistence/abstract')
const { Level } = require('level') // Level >= 8.0.0

const { randomUUID } = require('crypto')
const { tmpdir } = require('os')
const { join } = require('path')
const { mkdirSync } = require('fs')

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
    t.notOk(err, 'no error')
    const instance2 = persistence(db)
    instance2.subscriptionsByTopic('hello', (err, resubs) => {
      t.notOk(err, 'no error')
      t.deepEqual(resubs, [{
        clientId: client.id,
        topic: 'hello/#',
        qos: 1
      }, {
        clientId: client.id,
        topic: 'hello',
        qos: 1
      }])
      instance.destroy(t.end.bind(t))
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
    t.error(err)
    // When the client comes back online, aedes calls emptyQueue which calls outgoingUpdate
    instance.outgoingUpdate(client1, packet, (err, client, packet) => {
      t.notOk(err, 'no error')
      // When pubrel is published, outgoingUpdate is called again without the broker Id
      instance.outgoingUpdate(client, updatePacket, (err, client, packet) => {
        t.notOk(err, 'no error')
        instance.destroy(t.end.bind(t))
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
    t.notOk(err, 'no error')
    instance.addSubscriptions(client1, sub2, err => {
      t.notOk(err, 'no error')
      instance.subscriptionsByTopic('test/television/dev/about', (err, resubs) => {
        t.notOk(err, 'no error')
        t.deepEqual(resubs, [{
          topic: 'test/television/dev/about',
          clientId: 'test.1',
          qos: 1
        }, {
          topic: 'test/+/dev/#',
          clientId: 'test',
          qos: 2
        }])
        instance.destroy(t.end.bind(t))
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
    t.notOk(err, 'no error')
    instance.addSubscriptions(client, sub2, err => {
      t.notOk(err, 'no error')
      instance.subscriptionsByTopic('test/television/dev/about', (err, resubs) => {
        t.notOk(err, 'no error')
        t.deepEqual(resubs, [{
          topic: 'test/television/dev/about',
          clientId: 'test',
          qos: 1
        }])
        instance.destroy(t.end.bind(t))
      })
    })
  })
})
