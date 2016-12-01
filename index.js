function initLib (execlib) {
  return execlib.loadDependencies('client', ['allex:leveldb:lib'], require('./creator').bind(null, execlib));
}

module.exports = initLib;
