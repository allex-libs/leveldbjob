function createConfirmedMessageJobBase (execlib, LevelDBJobBase) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  function secondarryelementreturner (arry) {
    return q(arry[1]);
  }

  function ConfirmedMessageJobBase (ldbjob, defer) {
    LevelDBJobBase.call(this, ldbjob, defer);
    this.confirmed = null;
  }
  lib.inherit(ConfirmedMessageJobBase, LevelDBJobBase);
  ConfirmedMessageJobBase.prototype.destroy = function () {
    this.confirmed = null;
    LevelDBJobBase.prototype.destroy.call(this);
  };
  ConfirmedMessageJobBase.prototype.go = function () {
    var otg = this.okToGo();
    if (!otg.ok) {
      return otg.val;
    }
    this.getConfirmed().then(
      this.onConfirmed.bind(this),
      this.reject.bind(this)
    );
    return otg.val;
  };
  ConfirmedMessageJobBase.prototype.onConfirmed = function (confirmed) {
    this.confirmed = confirmed;
  };
  ConfirmedMessageJobBase.prototype.putConfirmed = function (toconfirm) {
    var ctg = this.clearToGo();
    if (!ctg.ok) {
      return ctg.val;
    }
    return this.ldbjob.syncs.put('confirmed', toconfirm).then(
      secondarryelementreturner
    );
  };

  return ConfirmedMessageJobBase;
}

module.exports = createConfirmedMessageJobBase;

