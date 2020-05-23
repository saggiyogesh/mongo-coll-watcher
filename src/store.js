/**
 * Module to store resume token for a ns in memory (obj) and periodically persist same in db.
 * This will prevent saving resume token in db when changes are very frequent.
 */
const Log = require('logger3000').getLogger(__filename);
const { getCollection } = require('native-mongo-util');
const schedule = require('node-schedule');

const { RESUME_TOKEN_PERSIST_INTERVAL = 30 /* 30 sec */ } = process.env;
const resumeTokenColl = '_resumeTokens';
const _obj = {};

exports.save = function (ns, token) {
  Log.debug({ msg: 'store save', arg1: ns, arg2: token });
  _obj[ns] = token;
};

exports.reset = function (ns) {
  delete _obj[ns];
};

exports.persistToken = function () {
  for (const [ns, token] of Object.entries(_obj)) {
    Log.debug({ msg: 'persistToken', arg1: ns, arg2: token });
    getCollection(resumeTokenColl)
      .updateOne({ ns }, { $set: { resumeToken: token, ns, date: new Date() } }, { upsert: true })
      .then(() => {
        exports.reset(ns);
        Log.debug({ msg: 'Resume token saved in db, ns: ' + ns });
      })
      .catch((e) =>
        Log.error({ error: e, msg: 'Error occurred while saving resume token, ns: ' + ns })
      );
  }
};

exports.init = function () {
  // If env var RESUME_TOKEN_PERSIST_INTERVAL is not passed, resume tokens will not persist periodically
  parseInt(RESUME_TOKEN_PERSIST_INTERVAL) &&
    schedule.scheduleJob(`*/${RESUME_TOKEN_PERSIST_INTERVAL} * * * * *`, function () {
      exports.persistToken();
    });
};
