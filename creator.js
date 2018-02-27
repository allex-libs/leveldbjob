function promisereturner (promiseobj) {
  return promiseobj.promise;
}

function createJob (execlib, leveldblib) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    DbJobBase = require('./jobs/dbjobbasecreator')(execlib),
    LevelDBJobBase = require('./jobs/leveldbjobbasecreator')(execlib),
    ConfirmedMessageJobBase = require('./jobs/confirmedmessagejobbasecreator')(execlib, LevelDBJobBase),
    BatchJob = require('./jobs/batchjobcreator')(execlib, DbJobBase),
    ConsistencyManagerJob = require('./jobs/consistencymanagerjobcreator')(execlib, ConfirmedMessageJobBase, BatchJob),
    MessagePutterJob = require('./jobs/messageputterjobcreator')(execlib, LevelDBJobBase),
    MessageFetcherJob = require('./jobs/messagefetcherjobcreator')(execlib, ConfirmedMessageJobBase, BatchJob),
    MessageConfirmatorJob = require('./jobs/messageconfirmatorjobcreator')(execlib, ConfirmedMessageJobBase, BatchJob);

  function LevelDBJob (prophash) {
    if (!(prophash && prophash.path)) {
      throw new lib.Error('NO_PATH_SPECIFIED', 'LevelDBJob ctor needs a prophash that has at least the `path` property');
    }
    this.path = prophash.path;
    this.fetchChunk = prophash.fetchchunk || 1;
    this.messages = null;
    this.syncs = null;
    this.locks = new qlib.JobCollection();
    this.fetchDefer = null;
    this.inputFinished = false;
    this.initLevelDBJob(prophash.jobusernames, prophash.starteddefer, prophash.initiallyemptydb);
  }
  LevelDBJob.prototype.destroy = function () {
    this.inputFinished = null;
    if (this.fetchDefer) {
      this.fetchDefer.resolve([]);
    }
    this.fetchDefer = null;
    if (this.locks) {
      this.locks.destroy();
    }
    this.locks = null;
    if (this.syncs) {
      this.syncs.destroy();
    }
    this.syncs = null;
    if (this.messages) {
      this.messages.destroy();
    }
    this.messages = null;
  };
  LevelDBJob.prototype.initLevelDBJob = function (jobusernames, starteddefer, initiallyemptydb) {
    var md = q.defer(),
      sd = q.defer(),
      promises = [md.promise, sd.promise];

    if (!lib.isArray(jobusernames)) {
      jobusernames = [];
    }
    new (leveldblib.DBArray)({
      dbname: [this.path, 'messages'],
      dbcreationoptions: {
        bufferValueEncoding: jobusernames.concat(['Byte', 'Byte'])  //was read, confirmed reception
      },
      indexsize: 'huge',
      initiallyemptydb: initiallyemptydb,
      starteddefer: md,
      startfromone: true
    });
    new (leveldblib.LevelDBHandler)({
      dbname: [this.path, 'syncs'],
      dbcreationoptions: {
        leveldbValueEncoding: 'UInt64BECodec'
      },
      initiallyemptydb: initiallyemptydb,
      starteddefer: sd
    });
    q.all(promises).then(
      this.onLevelDBJobDBs.bind(this, starteddefer)
    );
  };
  LevelDBJob.prototype.onLevelDBJobDBs = function (starteddefer, dbs) {
    this.messages = dbs[0];
    this.syncs = dbs[1];
    /*
    if (starteddefer) {
      starteddefer.resolve(this);
    }
    */
    (new ConsistencyManagerJob(this, starteddefer)).go();
  };
  LevelDBJob.prototype.store = function (messageobj) {
    //console.log('store?', messageobj);
    if (this.inputFinished) {
      return q.reject(new lib.Error('INPUT_FINISHED', 'Cannot store jobs any more, input is finished'));
    }
    return this.locks.run('messages', new qlib.PromiseChainerJob([
      this.createMessagePutterJob.bind(this, messageobj)
    ]));
  };

  LevelDBJob.prototype.createMessagePutterJob = function (messageobj) {
    var ret = this.locks.run('db', new MessagePutterJob(this, messageobj));
    messageobj = null;
    return ret;
  };

  LevelDBJob.prototype.fetch = function () {
    return this.locks.run('fetch', new qlib.PromiseChainerJob([
      this.fetchMessagesFromDB.bind(this),
      promisereturner
    ]));
  };

  LevelDBJob.prototype.fetchMessagesFromDB = function () {
    return this.locks.run('db', new MessageFetcherJob(this, this.fetchChunk));
  };

  LevelDBJob.prototype.confirm = function (msgid) {
    return this.locks.run('confirm', new (qlib.PromiseChainerJob)([
      this.confirmMessagesUpTo.bind(this, msgid),
      this.onConfirmDone.bind(this)
    ]));
  };

  LevelDBJob.prototype.confirmMessagesUpTo = function (msgid) {
    return this.locks.run('db', new MessageConfirmatorJob(this, msgid));
  };

  LevelDBJob.prototype.onConfirmDone = function (result) {
    if (!result) {
      return q([]);
    } else {
      return this.fetch();
    }
  };

  LevelDBJob.prototype.finishInput = function () {
    this.inputFinished = true;
    this.locks.run('messages', new qlib.PromiseChainerJob([
      this.finishFetchDefer.bind(this)
    ]));
  };

  LevelDBJob.prototype.finishFetchDefer = function () {
    var fd = this.fetchDefer;
    this.fetchDefer = null;
    if (fd) {
      fd.resolve([]);
    }
    return q(true);
  };


  LevelDBJob.addMethods = function (klass) {
    lib.inheritMethods(klass, LevelDBJob,
      'initLevelDBJob',
      'onLevelDBJobDBs',
      'store',
      'createMessagePutterJob',
      'fetch',
      'fetchMessagesFromDB',
      'confirm',
      'confirmMessagesUpTo',
      'onConfirmDone',
      'finishInput',
      'finishFetchDefer'
    );
  };

  return q(LevelDBJob);
}

module.exports = createJob;
