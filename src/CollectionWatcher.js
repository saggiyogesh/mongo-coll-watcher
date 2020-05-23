const Log = require('logger3000').getLogger(__filename);
const { getDBName, getCollection } = require('native-mongo-util');

const { save } = require('./store');

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

    this.changeStream.on('end', () => {
      Log.debug({ msg: 'change stream ended', arg1: this.getNS() });
    });

    this.changeStream.on('close', () => {
      Log.debug({ msg: 'change stream closed', arg1: this.getNS() });
    });

    this.changeStream.on('error', (err) => {
      Log.error({ error: err, arg1: this.getNS() });
      if (err.code === 40615) {
        Log.info({
          msg:
            'Error: Retry to initiate changeStream after deleting resume token for ns: ' +
            this.getNS(),
        });
      }
    });

    return { ns, changeStream: this.changeStream };
  }

  onChange(change) {
    if (change.operationType === 'invalidate') {
      Log.info({
        msg: 'Error: Got operationType `invalidate`, collection is renamed or dropped.',
        arg1: this.getNS(),
      });
      // closeAllChangeStreams(this.getNS())
      //   .then(() => process.exit(0))
      //   .catch(e => {
      //     Log.error({ error: e, msg: 'Error occurred while closing all change streams' });
      //     process.exit(0);
      //   });
    } else {
      const ns = this.getNS();
      const {
        _id,
        operationType,
        ns: { db, coll },
        documentKey,
        updateDescription,
        fullDocument,
      } = change;
      Log.debug({ msg: 'onchange, ns: ' + ns, arg1: { operationType, documentKey } });
      try {
        this.collListener({
          updateDescription,
          fullDocument,
          dbName: db,
          collectionName: coll,
          type: operationType,
          id: documentKey._id,
        });

        save(ns, _id);
      } catch (err) {
        Log.error({ error: err, msg: 'Error occurred while calling Listener for ns' + ns });
      }
    }
  }
};
