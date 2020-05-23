const Promise = require('bluebird');
const _watcherInstances = {};

exports.store = function (watchers) {
  for (const watcher of watchers) {
    _watcherInstances[watcher.ns] = watcher.changeStream;
  }
};

exports.closeChangeStream = async function (ns) {
  const r = await _watcherInstances[ns].close();
  console.log('closeChangeStream-->', ns, r);
};

exports.closeAllChangeStreams = async function (ignoredNS) {
  await Promise.map(
    Object.entries(_watcherInstances),
    ({ ns }) => !ignoredNS && exports.closeChangeStream(ns),
    {
      concurrency: 3,
    }
  );
};
