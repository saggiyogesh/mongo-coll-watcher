const Log = require('lil-logger').getLogger(__filename);
const { getDBName, getCollection } = require('native-mongo-util');

const resumeTokenColl = '_resumeTokens';
module.exports = class CollectionWatcher {
  constructor(collectionName, collListener) {
    this.collectionName = collectionName;
    this.collListener = collListener;
    this.dbName = getDBName();
  }

  getNS() {
    return `${this.dbName}.${this.collectionName}`;
  }

  async init() {
    const ns = this.getNS();
    Log.debug({ msg: 'Init watcher: ' + ns });
    const previousToken = await getCollection(resumeTokenColl).findOne({ ns });
    let opts = {};
    if (previousToken) {
      opts = { resumeAfter: previousToken.resumeToken };
      Log.debug({ msg: `Found previousToken, id: ${previousToken._id}, ns: ${ns}` });
    }
    this.changeStream = getCollection(this.collectionName).watch(opts);
    this.changeStream.on('change', this.onChange.bind(this));
  }

  onChange(change) {
    const ns = this.getNS();
    const {
      _id,
      operationType,
      ns: { db, coll },
      documentKey
    } = change;
    Log.debug({ msg: 'onchange, ns: ' + ns, arg1: change });
    try {
      this.collListener({ dbName: db, collectionName: coll, type: operationType, id: documentKey });

      getCollection(resumeTokenColl)
        .updateOne({ ns }, { $set: { resumeToken: _id, ns, date: new Date() } }, { upsert: true })
        .then(() => Log.debug({ msg: 'Resume token updated, ns: ' + ns }))
        .catch(e =>
          Log.error({ error: e, msg: 'Error occurred while saving resume token, ns: ' + ns })
        );
    } catch (err) {
      Log.error({ error: err, msg: 'Error occurred while calling Listener for ns' + ns });
    }
  }
};
