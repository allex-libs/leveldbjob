function messageisread (msg) {
  return msg[msg.length-2] == 1;
}

function messagecontents (msg) {
  if (msg.length===3) {
    return msg[0];
  }
  return msg.slice(0, msg.length-2);
}

function markread (msg) {
  msg[msg.length-2] = 1;
}

function createMessagePutterJob (execlib, LevelDbJobBase) {
  'use strict';

  var lib = execlib.lib;

  function array4put (messageobj, wasread) {
    if (lib.isArray(messageobj)) {
      return messageobj.concat(wasread, 0);
    }
    return [messageobj, wasread, 0];
  }

  function MessagePutterJob (ldbjob, messageobj, defer) {
    LevelDbJobBase.call(this, ldbjob.messages, defer);
    this.ldbjob = ldbjob;
    this.messageobj = messageobj;
  }
  lib.inherit(MessagePutterJob, LevelDbJobBase);
  MessagePutterJob.prototype.destroy = function () {
    this.ldbjob = null;
    this.messageobj = null;
    LevelDbJobBase.prototype.destroy.call(this);
  };
  MessagePutterJob.prototype.go = function () {
    var otg = this.okToGo();
    if (!otg.ok) {
      return otg.val;
    }
    this.ldbjob.messages.push(array4put(this.messageobj, this.ldbjob.fetchDefer ? 1 : 0)).then(
      this.onMessageStored.bind(this),
      this.reject.bind(this)
    );
    return otg.val;
  };
  MessagePutterJob.prototype.onMessageStored = function (stored) {
    if (!this.okToProceed()) {
      return;
    }
    this.getConfirmed().then(
      this.checkLastConfirmed.bind(this, stored),
      this.reject.bind(this)
    );
  };
  MessagePutterJob.prototype.checkLastConfirmed = function (stored, lc) {
    if (!this.okToProceed()) {
      return;
    }
    lc ++;
    if (stored[0] === lc) {
      //console.log(stored[0], '===', lc);
      var fd = this.ldbjob.fetchDefer;
      if (fd) {
        if (messageisread(stored[1])) {
          this.ldbjob.fetchDefer = null;
          fd.resolve([[lc, messagecontents(stored[1])]]);
        } else {
          //mark it read
          markread(stored[1]);
          return this.ldbjob.messages.put(stored[0], stored[1]).then(
            this.onMessageStored.bind(this)
          );
        }
      }
    }
    this.resolve(stored[0]);
  };

  return MessagePutterJob;
}

module.exports = createMessagePutterJob;
