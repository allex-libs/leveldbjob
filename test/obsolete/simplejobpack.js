var _cnt = 0;
function generator () {
  return 'Data '+(++_cnt);
}

module.exports = {
  path: 'simplejobtest.db',
  jobusernames: ['String'],
  generate: generator
};
