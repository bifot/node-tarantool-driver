/* global Promise */


const exec = require('child_process').exec;
const Benchmark = require('benchmark');

const suite = new Benchmark.Suite();
const Driver = require('../lib/connection.js');

let promises;

const conn = new Driver(process.argv[process.argv.length - 1], { lazyConnect: true });

conn.connect()
  .then(() => {
    suite.add('select cb', {
      defer: true,
      fn(defer) {
        function callback() {
          defer.resolve();
        }
        conn.selectCb(512, 0, 1, 0, 'eq', ['test'], callback, console.error);
      },
    });

    suite.add('select promise', {
      defer: true,
      fn(defer) {
        conn.select(512, 0, 1, 0, 'eq', ['test'])
          .then(() => { defer.resolve(); });
      },
    });

    suite.add('paralell 500', {
      defer: true,
      fn(defer) {
        try {
          promises = [];
          for (let l = 0; l < 500; l++) {
            promises.push(conn.select(512, 0, 1, 0, 'eq', ['test']));
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

    suite.add('paralel by 10', {
      defer: true,
      fn(defer) {
        let chain = Promise.resolve();
        try {
          for (let i = 0; i < 50; i++) {
            chain = chain.then(() => {
              promises = [];
              for (let l = 0; l < 10; l++) {
                promises.push(
                  conn.select(512, 0, 1, 0, 'eq', ['test']),
                );
              }
              return Promise.all(promises);
            });
          }

          chain.then(() => { defer.resolve(); })
            .catch((e) => {
              console.error(e, e.stack);
            });
        } catch (e) {
          console.error(e, e.stack);
        }
      },
    });

    suite.add('paralel by 50', {
      defer: true,
      fn(defer) {
        let chain = Promise.resolve();
        try {
          for (let i = 0; i < 10; i++) {
            chain = chain.then(() => {
              promises = [];
              for (let l = 0; l < 50; l++) {
                promises.push(
                  conn.select(512, 0, 1, 0, 'eq', ['test']),
                );
              }
              return Promise.all(promises);
            });
          }

          chain.then(() => { defer.resolve(); })
            .catch((e) => {
              console.error(e, e.stack);
            });
        } catch (e) {
          console.error(e, e.stack);
        }
      },
    });
    suite
      .on('cycle', (event) => {
        console.log(String(event.target));
      })
      .on('complete', () => {
        console.log('complete');
        process.exit();
      })
      .run({ async: true, queued: true });
  });
