process.env.MONGO_URL = 'mongodb://localhost/someDB';
process.env.NODE_ENV = 'production';

// import test from 'ava';
const { getCollection, connect } = require('native-mongo-util');
const sleep = require('then-sleep');
const watcher = require('./src');

const collName = 'testing';
const resumeTokenCollName = '_resumeToken';

// function watchPromised() {
//   return new Promise((resolve, reject) => {
//     try {
//       watcher([collName], data => {
//         console.log('changes', data);
//         resolve(data);
//       });
//     } catch (err) {
//       reject(err);
//     }
//   });
// }

// test.serial('connect db and drop colls', async t => {
//   await connect();
//   await getCollection(resumeTokenCollName).drop();
//   await getCollection(collName).drop();

//   console.log('colls cleared');

//   const coll = getCollection(collName);

//   watcher(['testing'], data => {
//     console.log('changes', data);
//     t.is(data, true);
//   });

//   setTimeout(async () => {
//     console.log('insert', await coll.insert({ a: 1, b: 2 }));
//   }, 1000);
// });

(async function() {
  try {
    await connect();
    watcher(['user', 'number'], data => {
      console.log('changes', data);
    });
  } catch (err) {}
})();
