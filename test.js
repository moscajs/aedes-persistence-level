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
