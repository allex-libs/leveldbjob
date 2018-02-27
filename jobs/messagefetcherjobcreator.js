function readnonconfirmedmessage (msg) {
  var ml = msg.length, ret = new Array(ml), i;
  for(i=0; i<ml-2; i++) {
    ret[i] = msg[i];
  }
  ret[i++] = 1;
  ret[i] = 0;
  return ret;
}

function messagecontents (msg) {
  if (msg.length===3) {
    return msg[0];
  }
  return msg.slice(0, msg.length-2);
}

function createMessageFetcherJob (execlib, ConfirmedMessageJobBase, BatchJob) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  function MessageFetcherJob (ldbjob, fetchchunk, defer) {
    ConfirmedMessageJobBase.call(this, ldbjob);
    this.fetchchunk = fetchchunk || 1;
    this.messages = [];
  }
  lib.inherit(MessageFetcherJob, ConfirmedMessageJobBase);
  MessageFetcherJob.prototype.destroy = function () {
    this.messages = null;
    this.fetchchunk = null;
    ConfirmedMessageJobBase.prototype.destroy.call(this);
  };

  MessageFetcherJob.prototype.onConfirmed = function (confirmed) {
    ConfirmedMessageJobBase.prototype.onConfirmed.call(this, confirmed);
    new BatchJob(this.ldbjob.messages, this.onRecord.bind(this), {
      gt: confirmed,
      lte: confirmed+this.fetchchunk
    }).go().then(
      this.onBatchWritten.bind(this),
      this.reject.bind(this)
    );
  };

  MessageFetcherJob.prototype.onRecord = function (record, batch) {
    var key = record.key, val = record.value, vl = val.length;
    //console.log('key', key, 'val', val);
    if (val[vl-1] === 0) {
      if (val[vl-2] === 0) {
        batch.put(key, readnonconfirmedmessage(val));
      }
      this.messages.push([key, messagecontents(val)]);
      //console.log('msgs', colobj.msgs);
    }
    return true;
  };

  MessageFetcherJob.prototype.onBatchWritten = function (batchresult) {
    var retmsgs;
    if (!this.okToProceed()) {
      return;
    }
    retmsgs = batchresult ? this.messages : [];
    if (retmsgs.length < 1 && !this.ldbjob.inputFinished) {
      if (!this.ldbjob.fetchDefer) {
        this.ldbjob.fetchDefer = q.defer();
      }
      this.resolve({promise: this.ldbjob.fetchDefer.promise});
      return;
    }
    this.resolve({promise: q(retmsgs)});
  };

  return MessageFetcherJob;
}

module.exports = createMessageFetcherJob;


