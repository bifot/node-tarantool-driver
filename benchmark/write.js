/* global Promise */


const Benchmark = require('benchmark');

const suite = new Benchmark.Suite();
const Driver = require('../lib/connection.js');

const conn = new Driver(process.argv[process.argv.length - 1], { lazyConnect: true });
let promises;
let c = 0;

conn.connect()
  .then(() => {
    suite.add('insert', {
      defer: true,
      fn(defer) {
        conn.insert('bench', [c++, { user: 'username', data: 'Some data.' }])
          .then(() => { defer.resolve(); })
          .catch((e) => {
            console.error(e, e.stack);
            defer.reject(e);
          });
      },
    });

    suite.add('insert parallel 50', {
      defer: true,
      fn(defer) {
        try {
          promises = [];
          for (let l = 0; l < 50; l++) {
            promises.push(conn.insert('bench', [c++, { user: 'username', data: 'Some data.' }]));
          }
          const chain = Promise.all(promises);
          chain.then(() => { defer.resolve(); })
            .catch((e) => {
              console.error(e, e.stack);
              defer.reject(e);
            });
        } catch (e) {
          defer.reject(e);
          console.error(e, e.stack);
        }
      },
    });
    suite
      .on('cycle', (event) => {
        console.log(String(event.target));
      })
      .on('complete', () => {
        conn.eval('return clear()')
          .then(() => {
            console.log('complete');
            process.exit();
          })
          .catch((e) => {
            console.error(e);
          });
      })
      .run({ async: true, queued: true });
  });
