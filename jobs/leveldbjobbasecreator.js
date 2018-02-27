function createLevelDbJobBase (execlib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    JobBase = qlib.JobBase;

  function LevelDBJobBase (ldbjob, defer) {
    JobBase.call(this, defer);
    this.ldbjob = ldbjob;
  }
  lib.inherit(LevelDBJobBase, JobBase);
  LevelDBJobBase.prototype.destroy = function () {
    this.ldbjob = null;
    JobBase.prototype.destroy.call(this);
  };
  LevelDBJobBase.prototype.clearToGo = function () {
    var ret = {
      ok: true,
      val: null
    };
    if (!this.defer) {
      ret.ok = false;
      ret.val = new lib.Error('ALREADY_DESTROYED');
      return ret;
    }
    if (!this.ldbjob) {
      ret.ok = false;
      ret.val = new lib.Error('NO_LDBJOB');
      return ret;
    }
    if (!this.ldbjob.syncs) {
      ret.ok = false;
      ret.val = new lib.Error('NO_LDBJOB_SYNCS');
      return ret;
    }
    if (!this.ldbjob.messages) {
      ret.ok = false;
      ret.val = new lib.Error('NO_LDBJOB_MESSAGES');
      return ret;
    }
    if (!this.ldbjob.syncs.db) {
      ret.ok = false;
      ret.val = new lib.Error('LDBJOB_SYNCS_DESTROYED');
      return ret;
    }
    if (!this.ldbjob.messages.db) {
      ret.ok = false;
      ret.val = new lib.Error('LDBJOB_MESSAGES_DESTROYED');
      return ret;
    }
    ret.val = this.defer.promise;
    return ret;
  };
  LevelDBJobBase.prototype.okToGo = function () {
    var ret = this.clearToGo();
    if (!ret.ok) {
      ret.val = q.reject(ret.val);
      return ret;
    }
    return ret;
  }
  LevelDBJobBase.prototype.okToProceed = function () {
    var ret = this.clearToGo();
    if (ret.ok) {
      return true;
    }
    this.reject(ret.val);
    return false;
  };
  LevelDBJobBase.prototype.getConfirmed = function () {
    var ctg = this.clearToGo();
    if (!ctg.ok) {
      return q.reject(ctg.val);
    }
    return this.ldbjob.syncs.safeGet('confirmed', 0);
  };


  return LevelDBJobBase;
}

module.exports = createLevelDbJobBase;
