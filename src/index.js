const Log = require('lil-logger').getLogger(__filename);
const { getDB } = require('native-mongo-util');
const CollectionWatcher = require('./CollectionWatcher');

function exit() {
  process.exit(1);
}

module.exports = async function(watchedColls, collListener) {
  try {
    const db = await getDB();
    db.on('close', e => {
      Log.error({ error: e, msg: 'Mongo connected closed' });
      exit();
    });
    db.on('error', e => {
      Log.error({ error: e, msg: 'Mongo connected error' });
      console.log('error---', e);
      exit();
    });

    for (const coll of watchedColls) {
      const watcher = new CollectionWatcher(coll, collListener);
      await watcher.init();
    }
  } catch (err) {
    console.log('catch err---', err);
    Log.error({ error: err, msg: 'Error occurred while watching' });
    exit();
  }
};
