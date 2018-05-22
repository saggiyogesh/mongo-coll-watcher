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
    const previousToken = await getCollection(resumeTokenColl).findOne({
      ns: this.getNS()
    });
    let opts = {};
    if (previousToken) {
      opts = { resumeAfter: previousToken.resumeToken };
    }
    this.changeStream = getCollection(this.collectionName).watch(opts);
    this.changeStream.on('change', this.onChange.bind(this));
  }

  onChange(change) {
    console.log('in change');
    const {
      _id,
      operationType,
      ns: { db, coll },
      documentKey
    } = change;
    console.log('onchange--', operationType, coll, documentKey);

    this.collListener({ dbName: db, collectionName: coll, type: operationType, id: documentKey });

    const ns = this.getNS();
    getCollection(resumeTokenColl)
      .updateOne({ ns }, { $set: { resumeToken: _id, ns, date: new Date() } }, { upsert: true })
      .catch(e => Log.error({ error: e, msg: 'Error occurred while saving resume token' }));
  }
};
