var Path = require('path'),
  shouldStopStorage = false,
  child_process = require('child_process');

//storage

function onExternalMessage (msg) {
  Job.store([msg.string, msg.number]);
}

function store () {
  var i, msggenerators=5, cp;
  for (i=0; i<msggenerators; i++) {
    console.log(Path.join(__dirname, 'proc', 'msgcreator.js'));
    cp = child_process.fork(Path.join(__dirname, 'proc', 'msgcreator.js'), [], {stdio:'inherit'});
    cp.on('message', onExternalMessage);
  }
}

/*
var _randomStrings = ['blah', 'abc', 'trah', 'lah', 'njah', 'cba', 'wut'];
function randomString () {
  return _randomStrings[Math.floor(Math.random()*_randomStrings.length)];
}

function randomUint32 () {
  return Math.floor(Math.random()*4e9);
}

function store() {
  var msgs, i;
  if (shouldStopStorage) {
    return;
  }
  msgs = 5*Math.random();
  for (i=0; i<msgs; i++) {
    Job.store([randomString(), randomUint32()]);
  }
  lib.runNext(store, 100*Math.random());
}

function storeFailure (reason) {
  console.error('could not store because', reason);
  process.exit(1);
}
*/

//////////////////////


//read

var lastConfirmed = 0;

function startReading () {
  Job.fetch().then(
    onFetched,
    fetchFailure
  );
}

function onFetched (dataarry) {
  var last, last2confirm, ffid, lfid;
  if (!lib.isArray(dataarry)) {
    console.error(dataarry, 'is not an Array');
    process.exit(1);
  }
  ffid = dataarry[0][0];
  lfid = dataarry[dataarry.length-1][0];
  console.log('fetched', ffid, '-', lfid);
  last = dataarry[dataarry.length-1];
  if (lib.isArray(last)) {
    last2confirm = last[0];
    if (last2confirm < 1) {
      last2confirm = 1;
    }
    //console.log('confirming', last2confirm-lastConfirmed);
    lastConfirmed = last2confirm;
    lib.runNext(doConfirm, Math.random()*(lfid-ffid === 99 ? 1000 : 3000));
  }
}

function doConfirm () {
  console.log('confirming', lastConfirmed);
  return Job.confirm(lastConfirmed).then(
    onFetched,
    confirmFailure
  );
}

function fetchFailure (reason) {
  console.error('could not fetch because', reason);
  process.exit(1);
}

function confirmFailure (reason) {
  console.error('could not confirm because', reason);
  process.exit(1);
}

describe ('Test Pressure', function () {
  it('load lib', function () {
    return setGlobal('LevelDBJob', require('../')(execlib));
  });
  it('Create LevelDBJob Instance', function () {
    var d = q.defer(), ret = d.promise;
    setGlobal('Job', new LevelDBJob({
      path: Path.join(__dirname, 'test.db'),
      jobusernames: ['String', 'UInt32LE'],
      fetchchunk: 100,
      initiallyemptydb: true,
      starteddefer: d
    }));
    return ret;
  });
  it('Start the storage proc', function () {
    store();
  });
  it('Start the reading proc', function () {
    startReading();
  });
  /*
  it('Stop the storage proc', function () {
    shouldStopStorage = true;
  });
  */
});
