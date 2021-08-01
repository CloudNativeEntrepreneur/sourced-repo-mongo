const EventEmitter = require('events').EventEmitter
const log = require('debug')('sourced-repo-mongo')
const MongoClient = require('mongodb').MongoClient
const url = require('url')
const util = require('util')

function Mongo () {
  this.client = null
  this.db = null
  EventEmitter.call(this)
}

util.inherits(Mongo, EventEmitter)

Mongo.prototype.connect = function connect (mongoUrl, database, options = {
  useNewUrlParser: true,
  useUnifiedTopology: true
}) {
  const self = this
  return new Promise((resolve, reject) => {
    self.on('connected', (db) => {
      resolve(db)
    })
    self.on('error', (err) => {
      reject(err)
    })
    MongoClient.connect(mongoUrl, options, function (err, client) {
      if (err) {
        log('✗ MongoDB Connection Error. Please make sure MongoDB is running: ', err)
        self.emit('error', err)
      }
      const expanded = url.parse(mongoUrl)
      // replica set url does not include db, it is passed in separately
      const dbName = database || expanded.pathname.replace('/', '')
      self.client = client
      const db = client.db(dbName)
      self.db = db
      log('initialized connection to mongo at %s', mongoUrl)
      self.emit('connected', db)
    })
  })
}

Mongo.prototype.close = function (cb) {
  log('closing sourced mongo connection')
  return this.client.close((err) => {
    log('closed sourced mongo connection')
    cb(err)
  })
}

module.exports = new Mongo()
