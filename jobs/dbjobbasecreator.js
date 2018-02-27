function createDbJobBase (execlib) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    JobBase = qlib.JobBase;

  function DbJobBase (db, defer) {
    if (!db) {
      console.trace();
      process.exit(1);
    }
    JobBase.call(this, defer);
    this.db = db;
  }
  lib.inherit(DbJobBase, JobBase);
  DbJobBase.prototype.destroy = function () {
    this.db = null;
    JobBase.prototype.destroy.call(this);
  };
  DbJobBase.prototype.clearToGo = function () {
    var ret = {
      ok: true,
      val: null
    };
    if (!this.defer) {
      ret.ok = false;
      ret.val = new lib.Error('ALREADY_DESTROYED');
      return ret;
    }
    if (!this.db) {
      ret.ok = false;
      ret.val = new lib.Error('ALREADY_DESTROYED');
      return ret;
    }
    if (!this.db.db) {
      ret.ok = false;
      ret.val = new lib.Error('DB_ALREADY_DESTROYED');
      return ret;
    }
    ret.val = this.defer.promise;
    return ret;
  };
  DbJobBase.prototype.okToGo = function () {
    var ctg = this.clearToGo();
    if (!ctg.ok) {
      ctg.val = q.reject(ctg.val);
    }
    return ctg;
  };
  DbJobBase.prototype.okToProceed = function () {
    var ctg = this.clearToGo();
    if (!ctg.ok) {
      this.reject(ctg.val);
      return false;
    }
    return true;
  };

  return DbJobBase;
}

module.exports = createDbJobBase;
