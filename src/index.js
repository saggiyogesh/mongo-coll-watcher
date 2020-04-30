const Promise = require('bluebird');
const Log = require('logger3000').getLogger(__filename);
const CollectionWatcher = require('./CollectionWatcher');
const { init, persistToken } = require('./store');
const { store: storeWatchers } = require('./watchers');

/**
 * Util to receive mongo collection changes, in realtime using mongo `ChangeStream`.
 * Expects that db is connected using `native-mongo-util` package.
 *
 * @param {Array} watchedColls - collection names to be watched for changes
 * @param {Function} collListener  - Common Listener to receive changes by watcher
 */
module.exports = async function(watchedColls, collListener) {
  try {
    Log.debug({ msg: 'Watched collections: ', arg1: watchedColls });

    const watchers = await Promise.map(watchedColls, coll => {
      const watcher = new CollectionWatcher(coll, collListener);
      return watcher.init();
    });

    storeWatchers(watchers);
    init();
  } catch (err) {
    Log.error({ error: err, msg: 'Error occurred while watching' });
    process.exit(1);
  }
};

exports.persistToken = persistToken;
