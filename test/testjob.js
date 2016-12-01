var tester = require('./'+process.argv[3]),
  _consumerid = 0,
  _consumercount = 0;

function Consumer (job) {
  _consumercount++;
  this.id = 'Consumer '+(++_consumerid);
  this.job = job;
}
Consumer.prototype.destroy = function () {
  console.log(this.id, 'dying');
  this.job = null;
  this.id = null;
  _consumercount--;
  if (_consumercount < 1) {
    process.exit(0);
  }
};
Consumer.prototype.go = function (id2confirm) {
  var p;
  if (!this.job) {
    return;
  }
  if (!id2confirm) {
    p = this.job.fetch();
  } else {
    console.log(this.id, 'confirming', id2confirm);
    p = this.job.confirm(id2confirm);
  }
  p.then(this.onFetched.bind(this));
};
Consumer.prototype.onFetched = function (msgs) {
  console.log(this.id, 'fetched', msgs);
  if (msgs && msgs.length) {
    this.go(msgs[msgs.length-1][0]);
  } else {
    this.destroy();
  }
};

function run (execlib, job) {
  var consumers = [
    new Consumer(job),
    new Consumer(job),
    new Consumer(job),
    new Consumer(job)
  ];
  consumers.forEach(function(c) {c.go();});
  store(execlib.lib.runNext, job);
  execlib.lib.runNext(finishInput.bind(null, job), 10*execlib.lib.intervals.Second);
}

function finishInput(job) {
  console.log('finishInput!');
  job.finishInput();
}

function store(runNext, job) {
  job.store(tester.generate()).then(null, null, process.exit.bind(process, 0));
  runNext(store.bind(null, runNext, job), 1000);
}

function go (execlib, LevelDBJob) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    d = q.defer(),
    job = new LevelDBJob({
      path: tester.path,
      jobusernames: tester.jobusernames,
      starteddefer: d
    });
  d.promise.then(run.bind(null, execlib));
}


function main (execlib) {
  require('../')(execlib).then(go.bind(null, execlib));
}

module.exports = main;

