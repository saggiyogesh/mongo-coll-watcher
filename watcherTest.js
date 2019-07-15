const { connect, getCollection, close } = require('native-mongo-util');
const sleep = require('then-sleep');
const watcher = require('./src');

const colName = 'bigColl';

const { type } = process.env;
if (!type) {
  throw new Error('Use `type` env var as `update`, `insert`, `delete` to test');
}

const total = 100000;
let startDate;
async function insertRecords(connectDB = true, closeDB = true) {
  connectDB &&
    (await connect({
      poolSize: 100
    }));

  const coll = getCollection(colName);
  const records = [];

  for (let i = 0; i < total; i++) {
    records.push({
      a: i * 100,
      b: i,
      c: 0
    });
  }

  console.log('inserting records', records.length);
  const r = await coll.insert(records);
  console.log('inserted', r.result);

  closeDB && (await close());
}

async function connectAndWatch(dropCol) {
  await connect({ poolSize: 200 });
  try {
    await getCollection('_resumeTokens').drop();
  } catch (err) {}

  if (dropCol) {
    try {
      await getCollection(colName).drop();
    } catch (err) {}
  }

  let i = 0;
  await watcher([colName], data => {
    if (data.type === type) {
      i++;
      console.log('i------------>', i);
    }
  });

  const it = setInterval(() => {
    if (i === total) {
      const d = new Date();
      console.log('End test', d);
      const time = `${(d.getTime() - startDate.getTime()) / 1000} seconds`;
      console.log('Total time taken', time);
      clearInterval(it);
      process.exit(0);
    }
  }, 1000);
}

(async function() {
  try {
    startDate = new Date();
    console.log('Start test', startDate);
    switch (type) {
      case 'insert':
        await connectAndWatch();
        await sleep(1000);
        await insertRecords(false, false);
        break;

      case 'update': {
        await insertRecords(total);
        await sleep(1000);
        await connectAndWatch(false);

        // update all records
        const coll = getCollection(colName);
        await coll.updateMany({ c: 0 }, { $set: { c: 1 } });
        break;
      }

      case 'delete': {
        await insertRecords(total);
        await sleep(1000);
        await connectAndWatch(false);

        // delete all records
        const coll = getCollection(colName);
        await coll.deleteMany({});
        break;
      }

      default:
        break;
    }
  } catch (err) {
    throw err;
  }
})();
