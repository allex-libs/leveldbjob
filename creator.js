var Path = require('path');

function createJob (execlib, leveldblib) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  //message handlers

  function readnonconfirmedmessage (msg) {
    var ml = msg.length, ret = new Array(ml), i;
    for(i=0; i<ml-2; i++) {
      ret[i] = msg[i];
    }
    ret[i++] = 1;
    ret[i] = 0;
    return ret;
  }

  function readmessage (msg) {
    var ml = msg.length, ret = new Array(ml), i;
    for(i=0; i<ml-2; i++) {
      ret[i] = msg[i];
    }
    ret[i++] = 1;
    ret[i] = msg[i];
    return ret;
  }
  function confirmedmessage (msg) {
    var ml = msg.length, ret = new Array(ml), i;
    for(i=0; i<ml-1; i++) {
      ret[i] = msg[i];
    }
    ret[i] = 1;
    return ret;
  }

  function messagecontents (msg) {
    if (msg.length===3) {
      return msg[0];
    }
    return msg.slice(0, msg.length-2);
  }

  function messageisread (msg) {
    return msg[msg.length-2] == 1;
  }
  function markread (msg) {
    msg[msg.length-2] = 1;
  }

  //

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
    this.initLevelDBJob(prophash.jobusernames, prophash.starteddefer);
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
  LevelDBJob.prototype.initLevelDBJob = function (jobusernames, starteddefer) {
    var md = q.defer(),
      sd = q.defer(),
      promises = [md.promise, sd.promise];

    if (!lib.isArray(jobusernames)) {
      jobusernames = [];
    }
    new (leveldblib.DBArray)({
      dbname: Path.join(this.path, 'messages'),
      dbcreationoptions: {
        bufferValueEncoding: jobusernames.concat(['Byte', 'Byte'])  //was read, confirmed reception
      },
      indexsize: 'huge',
      starteddefer: md,
      startfromone: true
    });
    new (leveldblib.LevelDBHandler)({
      dbname: Path.join(this.path, 'syncs'),
      dbcreationoptions: {
        leveldbValueEncoding: 'UInt64BECodec'
      },
      starteddefer: sd
    });
    q.all(promises).then(
      this.onLevelDBJobDBs.bind(this, starteddefer)
    );
  };
  LevelDBJob.prototype.onLevelDBJobDBs = function (starteddefer, dbs) {
    this.messages = dbs[0];
    this.syncs = dbs[1];
    if (starteddefer) {
      starteddefer.resolve(this);
    }
  };
  function array4put (messageobj, wasread) {
    if (lib.isArray(messageobj)) {
      return messageobj.concat(wasread, 0);
    }
    return [messageobj, wasread, 0];
  }
  LevelDBJob.prototype.store = function (messageobj) {
    //console.log('store?', messageobj);
    if (this.inputFinished) {
      return q.reject(new lib.Error('INPUT_FINISHED', 'Cannot store jobs any more, input is finished'));
    }
    return this.locks.run('messages', new qlib.PromiseChainerJob([
      this.messages.push.bind(this.messages, array4put(messageobj, this.fetchDefer ? 1 : 0)),
      this.onMessageStored.bind(this)
    ]));
  };

  LevelDBJob.prototype.onMessageStored = function (stored) {
    //console.log('stored', stored);
    //var id = stored[0], msgbuf = stored[1][0];
    return this.possiblyTrigger(stored);
  };

  LevelDBJob.prototype.possiblyTrigger = function (stored) {
    return this.syncs.safeGet('confirmed', 0).then(
      this.checkLastConfirmed.bind(this, stored)
    )
  };

  LevelDBJob.prototype.checkLastConfirmed = function (stored, lc) {
    //console.log('checking on', lc, 'which is', typeof lc);
    lc ++;
    if (stored[0] === lc) {
      //console.log(stored[0], '===', lc);
      var fd = this.fetchDefer;
      if (fd) {
        if (messageisread(stored[1])) {
          this.fetchDefer = null;
          //console.log('resolving with', [lc, stored[1][0]]);
          fd.resolve([[lc, messagecontents(stored[1])]]);
        } else {
          //mark it read
          markread(stored[1]);
          return this.messages.put(stored[0], stored[1]).then(
            this.onMessageStored.bind(this)
          );
        }
      }
    }
    return q(stored[0]);
  };

  LevelDBJob.prototype.fetch = function () {
    var promiseobj = {promise: null};
    return this.locks.run('fetch', new qlib.PromiseChainerJob([
      this.syncs.safeGet.bind(this.syncs, 'confirmed', 0),
      this.checkForConfirmed.bind(this),
      function (promise) {
        if (promise && 'object' === typeof promise && promise.hasOwnProperty('promise')) {
          promiseobj.promise = promise.promise;
        } else {
          promiseobj.promise = promise;
        }
        return q(true);
      }
    ])).then(
      qlib.propertyreturner(promiseobj, 'promise')
    );
  };

  function messagecollector(colobj, keyval) {
    var key = keyval.key, val = keyval.value, vl = val.length;
    //console.log('key', key, 'val', val);
    if (val[vl-1] === 0) {
      if (val[vl-2] === 0) {
        colobj.fetchmarkbatch.put(key, readnonconfirmedmessage(val));
      }
      colobj.msgs.push([key, messagecontents(val)]);
      //console.log('msgs', colobj.msgs);
    }
  }

  LevelDBJob.prototype.checkForConfirmed = function (confirmed) {
    //console.log('checkForConfirmed', confirmed);
    confirmed ++;
    var colobj = {msgs: [], fetchmarkbatch: this.messages.db.batch()};
    return this.messages.traverse(messagecollector.bind(null, colobj), {gte: confirmed, lt: confirmed+this.fetchChunk}).then(
      this.onMessagesConfirmedForFetch.bind(this, colobj)
    );
  };

  LevelDBJob.prototype.onMessagesConfirmedForFetch = function (colobj) {
    //console.log('fetched', colobj.msgs.length, 'msgs');
    if (colobj.msgs.length < 1 && !this.inputFinished) {
      if (!this.fetchDefer) {
        this.fetchDefer = q.defer();
      }
      return this.fetchDefer.promise;
    }
    var d = q.defer();
    colobj.fetchmarkbatch.write(fetchmarkbatchwritereporter.bind(null, d, colobj.msgs));
    return d.promise;
  };

  function fetchmarkbatchwritereporter(defer, msgs, error) {
    if (error) {
      console.error('Error in writing the "read" flags', error);
      defer.resolve([]);
      return;
    }
    defer.resolve(msgs);
    defer = null;
    msgs = null;
    error = null;
  }

  function afterreadupdatepacker(result) {
    return q([result[0], messagecontents(result[1])]);
  }

  LevelDBJob.prototype.confirm = function (msgid) {
    return this.locks.run('confirm', new (qlib.PromiseChainerJob)([
      this.syncs.safeGet.bind(this.syncs, 'confirmed', 0),
      this.messageFetcher.bind(this, msgid)
    ]));
  };

  function fetcherforconfirm(okobj, batch, keyval) {
    var key = keyval.key, msg = keyval.value;
    //console.log('fetcherforconfirm', okobj.ok, key, msg);
    if (!msg) {
      okobj.ok = false;
      return;
    }
    if (msg[msg.length-2] !== 1) {
      console.log(key, 'not read');
      okobj.ok = false;
      return;
    }
    if (msg[msg.length-1] !== 0) {
      console.log(key, 'already confirmed');
      okobj.ok = false;
      return;
    }
    if (okobj.ok !== false) {
      okobj.ok = true;
    }
    batch.put(key, confirmedmessage(msg));
  }
  function fetcherexecutor(okobj, batch) {
    var d;
    if (okobj.ok === true) {
      d = q.defer();
      batch.write(fetcherexecutorreporter.bind(null, d));
      return d.promise;
    } else {
      return q(false);
    }
  }
  function fetcherexecutorreporter (defer, error) {
    if (error) {
      console.log('updating messages with read flag => 1 failed', error);
    }
    defer.resolve(!error);
    defer = null;
    error = null;
  }
  LevelDBJob.prototype.messageFetcher = function (msgid, confirmed) {
    var batch = this.messages.db.batch(), okobj = {ok: null};
    return this.messages.traverse(fetcherforconfirm.bind(null, okobj, batch), {
      gt: confirmed,
      lte: msgid
    }).then(
      fetcherexecutor.bind(null, okobj, batch)
    ).then(
      this.doUpdateConfirm.bind(this, msgid)
    ).then(
      this.onConfirmDone.bind(this)
    );
  };

  LevelDBJob.prototype.doUpdateConfirm = function (msgid, result) {
    if (result) {
      //console.log('setting confirmed to', msgid);
      return this.syncs.put('confirmed', msgid);
    } else {
      return q(0);
    }
  };

  LevelDBJob.prototype.onConfirmDone = function (result) {
    //console.log('onConfirmDone', result);
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
      'onMessageStored',
      'possiblyTrigger',
      'checkLastConfirmed',
      'fetch',
      'checkForConfirmed',
      'onMessagesConfirmedForFetch',
      'confirm',
      'messageFetcher',
      'doUpdateConfirm',
      'onConfirmDone',
      'finishInput',
      'finishFetchDefer'
    );
  };

  return q(LevelDBJob);
}

module.exports = createJob;
