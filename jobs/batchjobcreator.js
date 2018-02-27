function createBatchJob (execlib, DbJobBase) {
  'use strict';

  var lib = execlib.lib;

  function BatchJob (db, recordproc, options, defer) {
    DbJobBase.call(this, db, defer);
    this.recordproc = recordproc;
    this.options = options;
    this.batch = null;
    this.ok = null;
  }
  lib.inherit(BatchJob, DbJobBase);
  BatchJob.prototype.destroy = function () {
    this.ok = null;
    this.batch = null;
    this.options = null;
    this.recordproc = null;
    DbJobBase.prototype.destroy.call(this);
  };
  BatchJob.prototype.go = function () {
    var otg = this.okToGo();
    if (!otg.ok) {
      return otg.val;
    }
    if (this.batch) {
      return otg.val;
    }
    this.batch = this.db.db.batch();
    this.db.traverse(this.onRecord.bind(this), this.options).then(
      this.onTraverseDone.bind(this),
      this.reject.bind(this)
    );
    return otg.val;
  };
  BatchJob.prototype.onRecord = function (record) {
    var ok;
    if (this.ok === false) {
      return;
    }
    ok = this.recordproc(record, this.batch);
    if (ok === false) {
      this.ok = false;
      return;
    }
    if (this.ok !== false) {
      this.ok = true;
    }
  };
  BatchJob.prototype.onTraverseDone = function () {
    if (!this.okToProceed()) {
      return;
    }
    if (!this.batch) {
      this.resolve(true);
      return;
    }
    if (this.ok === null) {
      this.resolve(true);
      return;
    }
    if (this.ok === true) {
      this.batch.write(this.onBatchWritten.bind(this));
      return;
    }
    this.resolve(false);
  };
  BatchJob.prototype.onBatchWritten = function (error) {
    if (error) {
      console.log('Error in running the batch', error);
    }
    this.resolve(!error); //most problematic!
  };

  return BatchJob;
}

module.exports = createBatchJob;
