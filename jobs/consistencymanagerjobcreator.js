function createConsistencyManagerJob (execlib, ConfirmedMessageJobBase) {
  'use strict';

  var lib = execlib.lib;

  function ConsistencyManagerJob (ldbjob, defer) {
    ConfirmedMessageJobBase.call(this, ldbjob, defer);
    this.targetconfirmed = null;
  }
  lib.inherit(ConsistencyManagerJob, ConfirmedMessageJobBase);
  ConsistencyManagerJob.prototype.destroy = function () {
    this.targetconfirmed = null;
    ConfirmedMessageJobBase.prototype.destroy.call(this);
  };
  ConsistencyManagerJob.prototype.onConfirmed = function (confirmed) {
    ConfirmedMessageJobBase.prototype.onConfirmed.call(this, confirmed);
    if (!this.okToProceed()) {
      return;
    }
    this.targetconfirmed = confirmed;
    this.ldbjob.messages.traverse(this.onRecord.bind(this)).then(
      this.onMessagesTraversed.bind(this),
      this.reject.bind(this)
    );
  };
  ConsistencyManagerJob.prototype.onRecord = function (record) {
    var key = record.key, val = record.value;
    if (key>this.confirmed && val[val.length-1] == 1) {
      if (key === this.targetconfirmed+1) {
        //acceptable
        this.targetconfirmed++;
      } else {
        if (this.targetconfirmed) {
          console.error('Consistency is broken, problematic message db key is', key, 'against my targetconfirmed', this.targetconfirmed);
        }
        this.targetconfirmed = null;
      }
    }
  };
  ConsistencyManagerJob.prototype.onMessagesTraversed = function () {
    if (this.targetconfirmed === null) {
      //consistency broken!
      this.reject(new lib.Error('INCONSISTENT_MESSAGES', 'The messages database is inconsistent'));
      return;
    }
    if (this.targetconfirmed > this.confirmed) {
      console.log('Fixing the inconsistency, moving', this.confirmed, 'to', this.targetconfirmed);
      this.putConfirmed(this.targetconfirmed).then(
        this.resolve.bind(this, true),
        this.reject.bind(this)
      );
      return;
    }
    this.resolve(true);
  };

  return ConsistencyManagerJob;
}

module.exports = createConsistencyManagerJob;
