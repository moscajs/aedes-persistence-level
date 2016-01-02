'use strict'

var test = require('tape').test
var persistence = require('./')
var abs = require('aedes-persistence/abstract')
var memdb = require('memdb')

abs({
  test: test,
  persistence: function () {
    return persistence(memdb())
  }
})

test('restore', function (t) {
  var db = memdb()
  var instance = persistence(db)
  var client = {
    id: 'abcde'
  }

  var subs = [{
    topic: 'hello',
    qos: 1
  }, {
    topic: 'hello/#',
    qos: 1
  }, {
    topic: 'matteo',
    qos: 1
  }]

  instance.addSubscriptions(client, subs, function (err) {
    t.notOk(err, 'no error')
    var instance2 = persistence(db)
    instance2.subscriptionsByTopic('hello', function (err, resubs) {
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
