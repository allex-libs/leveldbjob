
var _randomStrings = ['blah', 'abc', 'trah', 'lah', 'njah', 'cba', 'wut'];
function randomString () {
  return _randomStrings[Math.floor(Math.random()*_randomStrings.length)];
}

function randomUint32 () {
  return Math.floor(Math.random()*4e9);
}


function generateMessages () {
  var msgs, i;
  msgs = 5*Math.random();
  for (i=0; i<msgs; i++) {
    process.send({string: randomString(), number: randomUint32()});
  }
  setTimeout(generateMessages, 500*Math.random());
}

generateMessages();
