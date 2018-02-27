function confirmedmessage (msg) {
  var ml = msg.length, ret = new Array(ml), i;
  for(i=0; i<ml-1; i++) {
    ret[i] = msg[i];
  }
  ret[i] = 1;
  return ret;
}

function fetcherexecutorreporter (defer, error) {
  if (error) {
    console.log('updating messages with read flag => 1 failed', error);
  }
  defer.resolve(!error);
  defer = null;
  error = null;
}

function createMessageConfirmator (execlib, ConfirmedMessageJobBase, BatchJob) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  function MessageConfirmatorJob (ldbjob, tomsgid, defer) {
    ConfirmedMessageJobBase.call(this, ldbjob, defer);
    this.tomsgid = tomsgid;
  }
  lib.inherit(MessageConfirmatorJob, ConfirmedMessageJobBase);
  MessageConfirmatorJob.prototype.destroy = function () {
    this.tomsgid = null;
    ConfirmedMessageJobBase.prototype.destroy.call(this);
  };
  MessageConfirmatorJob.prototype.onConfirmed = function (confirmed) {
    ConfirmedMessageJobBase.prototype.onConfirmed.call(this, confirmed);
    new BatchJob(this.ldbjob.messages, this.onRecord.bind(this), {
      gt: confirmed,
      lte: this.tomsgid
    }).go().then(
      this.onMessagesConfirmedUpdated.bind(this),
      this.reject.bind(this)
    );
  };
  MessageConfirmatorJob.prototype.onRecord = function (keyval, batch) {
    var key, msg;
    if (!this.okToProceed()) {
      return false;
    }
    key = keyval.key;
    msg = keyval.value;
    //console.log('fetcherforconfirm', key, msg);
    if (!msg) {
      return false;
    }
    if (msg[msg.length-2] !== 1) {
      console.log(key, 'not read');
      return false;
    }
    if (msg[msg.length-1] !== 0) {
      console.log(key, 'already confirmed');
      return false;
    }
    batch.put(key, confirmedmessage(msg));
    return true;
  };
  MessageConfirmatorJob.prototype.onMessagesConfirmedUpdated = function (batchresult) {
    if (!this.okToProceed()) {
      return;
    }
    if (!batchresult) {
      this.resolve(0);
    }
    if (this.tomsgid>this.confirmed) {
      qlib.promise2defer(this.putConfirmed(this.tomsgid), this);
      return;
    }
    this.resolve(this.confirmed);
  };

  return MessageConfirmatorJob;
}

module.exports = createMessageConfirmator;
