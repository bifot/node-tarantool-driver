/**
 * Created by klond on 05.04.15.
 */

/* eslint-env mocha */
/* global Promise */
const exec = require('child_process').exec;
const expect = require('chai').expect;
const sinon = require('sinon');

const spy = sinon.spy.bind(sinon);
const stub = sinon.stub.bind(sinon);

const fs = require('fs');
const assert = require('assert');
const mlite = require('msgpack-lite');
const TarantoolConnection = require('../lib/connection');

let conn;

describe('constructor', () => {
  it('should parse options correctly', () => {
    stub(TarantoolConnection.prototype, 'connect').returns(Promise.resolve());
    let option;
    try {
      option = getOption(6380);
      expect(option).to.have.property('port', 6380);
      expect(option).to.have.property('host', 'localhost');

      option = getOption('6380');
      expect(option).to.have.property('port', 6380);

      option = getOption(6381, '192.168.1.1');
      expect(option).to.have.property('port', 6381);
      expect(option).to.have.property('host', '192.168.1.1');

      option = getOption(6381, '192.168.1.1', {
        password: '123',
        username: 'userloser',
      });
      expect(option).to.have.property('port', 6381);
      expect(option).to.have.property('host', '192.168.1.1');
      expect(option).to.have.property('password', '123');
      expect(option).to.have.property('username', 'userloser');

      option = getOption('mail.ru:33013');
      expect(option).to.have.property('port', 33013);
      expect(option).to.have.property('host', 'mail.ru');

      option = getOption('notguest:sesame@mail.ru:3301');
      expect(option).to.have.property('port', 3301);
      expect(option).to.have.property('host', 'mail.ru');
      expect(option).to.have.property('username', 'notguest');
      expect(option).to.have.property('password', 'sesame');

      option = getOption({
        port: 6380,
        host: '192.168.1.1',
      });
      expect(option).to.have.property('port', 6380);
      expect(option).to.have.property('host', '192.168.1.1');

      option = getOption({
        port: 6380,
        host: '192.168.1.1',
        reserveHosts: ['notguest:sesame@mail.ru:3301', 'mail.ru:3301'],
      });
      expect(option).to.have.property('port', 6380);
      expect(option).to.have.property('host', '192.168.1.1');
      expect(option).to.have.property('reserveHosts');
      expect(option.reserveHosts).to.deep.equal(['notguest:sesame@mail.ru:3301', 'mail.ru:3301']);

      option = new TarantoolConnection({
        port: 6380,
        host: '192.168.1.1',
        reserveHosts: ['notguest:sesame@mail.ru:3301', 'mail.ru:3301'],
      });
      expect(option.reserve).to.deep.include(
        {
          port:	6380,
          host: '192.168.1.1',
          username: null,
          password: null,
        },
        {
          port:	3301,
          host: 'mail.ru',
        },
        {
          port:	3301,
          host: 'mail.ru',
          username: 'notguest',
          password: 'sesame',
        },
      );

      option = getOption({
        port: 6380,
        host: '192.168.1.1',
      });
      expect(option).to.have.property('port', 6380);
      expect(option).to.have.property('host', '192.168.1.1');

      option = getOption({
        port: '6380',
      });
      expect(option).to.have.property('port', 6380);

      option = getOption(6380, {
        host: '192.168.1.1',
      });
      expect(option).to.have.property('port', 6380);
      expect(option).to.have.property('host', '192.168.1.1');

      option = getOption('6380', {
        host: '192.168.1.1',
      });
      expect(option).to.have.property('port', 6380);
    } catch (err) {
      TarantoolConnection.prototype.connect.restore();
      throw err;
    }
    TarantoolConnection.prototype.connect.restore();

    function getOption() {
      conn = TarantoolConnection(...arguments);
      return conn.options;
    }
  });

  it('should throw when arguments is invalid', () => {
    expect(() => {
      new TarantoolConnection((() => {}));
    }).to.throw(Error);
  });
});

describe('reconnecting', function () {
  this.timeout(8000);
  it('should pass the correct retry times', (done) => {
    let t = 0;
    new TarantoolConnection({
      port: 1,
      retryStrategy(times) {
        expect(times).to.eql(++t);
        if (times === 3) {
          done();
          return;
        }
        return 0;
      },
    });
  });

  it('should skip reconnecting when retryStrategy doesn\'t return a number', (done) => {
    conn = new TarantoolConnection({
      port: 1,
      retryStrategy() {
        process.nextTick(() => {
          expect(conn.state).to.eql(32); // states.END == 32
          done();
        });
        return null;
      },
    });
  });

  it('should not try to reconnect when disconnected manually', (done) => {
    conn = new TarantoolConnection(33013, { lazyConnect: true });
    conn.eval('return func_foo()')
      .then(() => {
        conn.disconnect();
        return conn.eval('return func_foo()');
      })
      .catch((err) => {
        expect(err.message).to.match(/Connection is closed/);
        done();
      });
  });
  it('should try to reconnect and then connect eventially', (done) => {
    function timer() {
      return conn.ping()
        .then((res) => {
          assert.equal(res, true);
          done();
        })
        .catch((err) => {
          done(err);
        });
    }
    conn = new TarantoolConnection(33013, { lazyConnect: true });
    conn.eval('return func_foo()')
      .then(() => {
        exec('docker kill tarantool', (error, stdout, stderr) => {
          if (error) {
            done(error);
          }
          conn.eval('return func_foo()')
            .catch((err) => {
              expect(err.message).to.match(/connect ECONNREFUSED/);
            });
          exec('docker start tarantool', (e, stdo, stde) => {
            if (error) {
              done(error);
            }
            setTimeout(timer, 1000);
          });
        });
      });
  });
});

describe('multihost', function () {
  this.timeout(10000);
  // after(function() {
  //   exec('docker start tarantool');
  // });
  let t;
  it('should try to connect to reserve hosts cyclically', (done) => {
    conn = new TarantoolConnection(33013, {
      reserveHosts: ['test:test@127.0.0.1:33014', '127.0.0.1:33015'],
      beforeReserve: 1,
      retryStrategy(times) {
        return Math.min(times * 500, 2000);
    	},
    });
    t = 0;
    conn.on('connect', () => {
      switch (t) {
        case 1:
          conn.eval('return box.cfg')
            .then((res) => {
              t++;
              expect(res[0].listen).to.eql('33014');
              exec('docker kill reserve', (error, stdout, stderr) => {
                if (error) {
                  done(error);
                }
              });
              exec('docker start tarantool');
            })
            .catch((e) => {
              done(e);
            });
          break;
        case 2:
          conn.eval('return box.cfg')
            .then((res) => {
              t++;
              expect(res[0].listen).to.eql('33015');
              exec('docker kill reserve_2', (error, stdout, stderr) => {
                if (error) {
                  done(error);
                }
              });
            })
            .catch((e) => {
              done(e);
            });
          break;
        case 3:
          conn.eval('return box.cfg')
            .then((res) => {
              t++;
              expect(res[0].listen).to.eql('33013');
              done();
            })
            .catch((e) => {
              done(e);
            });
          break;
      }
    });
    conn.ping()
      .then(() => {
        t++;
        exec('docker kill tarantool', (error, stdout, stderr) => {
          if (error) {
            done(error);
          }
        });
      })
      .catch((e) => {
        done(e);
      });
  });
});

describe('lazy connect', () => {
  beforeEach(() => {
    conn = new TarantoolConnection({
      port: 33013, lazyConnect: true, username: 'test', password: 'test',
    });
  });
  it('lazy connect', (done) => {
    conn.connect()
      .then(() => {
        done();
      }, (e) => {
        done(e);
      });
  });
  it('should be authenticated', (done) => {
    conn.connect().then(() => conn.eval('return box.session.user()'))
      .then((res) => {
        assert.equal(res[0], 'test');
        done();
      })
      .catch((e) => { done(e); });
  });
  it('should disconnect when inited', (done) => {
    conn.disconnect();
    expect(conn.state).to.eql(32); // states.END == 32
    done();
  });
  it('should disconnect', (done) => {
    conn.connect()
      .then((res) => {
        conn.disconnect();
        assert.equal(conn.socket.writable, false);
        done();
      })
      .catch((e) => { done(e); });
  });
});
describe('instant connection', () => {
  beforeEach(() => {
    conn = new TarantoolConnection({ port: 33013, username: 'test', password: 'test' });
  });
  it('connect', (done) => {
    conn.eval('return func_arg(...)', 'connected!')
      .then((res) => {
        try {
          assert.equal(res, 'connected!');
        } catch (e) { console.error(e); }
        done();
      }, (e) => {
        done(e);
      });
  });
  it('should reject when connected', (done) => {
    conn.connect().catch((err) => {
      expect(err.message).to.match(/Tarantool is already connecting\/connected/);
      done();
    });
  });
  it('should be authenticated', (done) => {
    conn.eval('return box.session.user()')
      .then((res) => {
        assert.equal(res[0], 'test');
        done();
      })
      .catch((e) => { done(e); });
  });
  it('should reject when auth failed', (done) => {
    conn = new TarantoolConnection({ port: 33013, username: 'userloser', password: 'test' });
    conn.eval('return func_foo()')
      .catch((err) => {
        expect(err.message).to.match(/User 'userloser' is not found/);
        conn.disconnect();
        done();
      });
  });
  it('should reject command when connection is closed', (done) => {
    conn = new TarantoolConnection();
    conn.disconnect();
    conn.eval('return func_foo()')
      .catch((err) => {
        expect(err.message).to.match(/Connection is closed/);
        done();
      });
  });
});

describe('timeout', () => {
  it('should close the connection when timeout', (done) => {
    conn = new TarantoolConnection(33013, '192.0.0.0', {
      timeout: 1,
      retryStrategy: null,
    });
    let pending = 2;
    conn.on('error', (err) => {
      expect(err.message).to.eql('connect ETIMEDOUT');
      if (!--pending) {
        done();
      }
    });
    conn.ping()
      .catch((err) => {
        expect(err.message).to.match(/Connection is closed/);
        if (!--pending) {
          done();
        }
      });
  });
  it('should clear the timeout when connected', (done) => {
    conn = new TarantoolConnection(33013, { timeout: 10000 });
    setImmediate(() => {
      stub(conn.socket, 'setTimeout')
        .callsFake((timeout) => {
          expect(timeout).to.eql(0);
          conn.socket.setTimeout.restore();
          done();
        });
    });
  });
});


describe('requests', () => {
  const insertTuple = [50, 10, 'my key', 30];
  before((done) => {
    console.log('before call');
    try {
      conn = new TarantoolConnection({ port: 33013, username: 'test', password: 'test' });

      Promise.all([conn.delete(514, 0, [1]), conn.delete(514, 0, [2]),
        conn.delete(514, 0, [3]), conn.delete(514, 0, [4]),
        conn.delete(512, 0, [999])])
        .then(() => conn.call('clearaddmore'))
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    } catch (e) {
      console.log(e);
    }
  });
  it('replace', (done) => {
    conn.replace(512, insertTuple)
      .then((a) => {
        assert.equal(a.length, 1);
        for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
        done();
      }, (e) => { done(e); });
  });
  it('simple select', (done) => {
    conn.select(512, 0, 1, 0, 'eq', [50])
      .then((a) => {
        assert.equal(a.length, 1);
        for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
        done();
      }, (e) => { done(e); });
  });
  it('simple select with callback', (done) => {
    conn.selectCb(512, 0, 1, 0, 'eq', [50], (a) => {
      assert.equal(a.length, 1);
      for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
      done();
    }, (e) => { done(e); });
  });
  it('composite select', (done) => {
    conn.select(512, 1, 1, 0, 'eq', [10, 'my key'])
      .then((a) => {
        assert.equal(a.length, 1);
        for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
        done();
      }).catch((e) => { done(e); });
  });
  it('delete', (done) => {
    conn.delete(512, 0, [50])
      .then((a) => {
        assert.equal(a.length, 1);
        for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
        done();
      }).catch((e) => { done(e); });
  });
  it('insert', (done) => {
    conn.insert(512, insertTuple)
      .then((a) => {
        assert.equal(a.length, 1);
        for (let i = 0; i < a[0].length; i++) { assert.equal(a[0][i], insertTuple[i]); }
        done();
      }, (e) => { done(e); });
  });
  it('dup error', (done) => {
    conn.insert(512, insertTuple)
      .then((a) => {
        done(new Error('can insert'));
      }, (e) => {
        assert(e instanceof Error);
        done();
      });
  });
  it('update', (done) => {
    conn.update(512, 0, [50], [['+', 3, 10]])
      .then((a) => {
        assert.equal(a.length, 1);
        assert.equal(a[0][3], insertTuple[3] + 10);
        done();
      }).catch((e) => { done(e); });
  });
  it('a lot of insert', (done) => {
    const promises = [];
    for (let i = 0; i <= 5000; i++) {
      promises.push(conn.insert(515, [`key${i}`, i]));
    }
    Promise.all(promises)
      .then((pr) => {
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
  it('check errors', (done) => {
    conn.insert(512, ['key', 'key', 'key'])
      .then(() => {
        done(new Error('Right when need error'));
      })
      .catch((e) => {
        done();
      });
  });
  it('call print', (done) => {
    conn.call('myprint', ['test'])
      .then(() => {
        done();
      })
      .catch((e) => {
        console.log(e);
        done(e);
      });
  });
  it('call batch', (done) => {
    conn.call('batch', [[1], [2], [3]])
      .then(() => {
        done();
      })
      .catch((e) => {
        console.log(e);
        done(e);
      });
  });
  it('call get', (done) => {
    conn.insert(514, [4])
      .then(() => conn.call('myget', 4))
      .then((value) => {
        done();
      })
      .catch((e) => {
        console.log(e);
        done(e);
      });
  });
  it('get metadata space by name', (done) => {
    conn._getSpaceId('batched')
      .then((v) => {
        assert.equal(v, 514);
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
  it('get metadata index by name', (done) => {
    conn._getIndexId(514, 'primary')
      .then((v) => {
        assert.equal(v, 0);
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
  it('insert with space name', (done) => {
    conn.insert('test', [999, 999, 'fear'])
      .then((v) => {
        done();
      })
      .catch(done);
  });
  it('select with space name and index name', (done) => {
    conn.select('test', 'primary', 0, 0, 'all', [999])
      .then(() => {
        done();
      })
      .catch(done);
  });
  it('select with space name and index number', (done) => {
    conn.select('test', 0, 0, 0, 'eq', [999])
      .then(() => {
        done();
      })
      .catch(done);
  });
  it('select with space number and index name', (done) => {
    conn.select(512, 'primary', 0, 0, 'eq', [999])
      .then(() => {
        done();
      })
      .catch(done);
  });
  it('delete with name', (done) => {
    conn.delete('test', 'primary', [999])
      .then(() => {
        done();
      })
      .catch(done);
  });
  it('update with name', (done) => {
    conn.update('test', 'primary', [999], ['+', 1, 10])
      .then(() => {
        done();
      })
      .catch(done);
  });
  it('evaluate expression', (done) => {
    conn.eval('return 2+2')
      .then((res) => {
        assert.equal(res, 4);
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
  it('evaluate expression with args', (done) => {
    conn.eval('return func_sum(...)', 11, 22)
      .then((res) => {
        assert.equal(res, 33);
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
  it('ping', (done) => {
    conn.ping()
      .then((res) => {
        assert.equal(res, true);
        done();
      })
      .catch((e) => {
        done(e);
      });
  });
});


describe('upsert', () => {
  before((done) => {
    try {
      conn = new TarantoolConnection({ port: 33013, lazyConnect: true });
      conn.connect().then(() => conn._auth('test', 'test'), (e) => { done(e); })
        .then(() => Promise.all([
          conn.delete('upstest', 'primary', 1),
          conn.delete('upstest', 'primary', 2),
        ]))
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    } catch (e) {
      console.log(e);
    }
  });
  it('insert', (done) => {
    conn.upsert('upstest', [['+', 3, 3]], [1, 2, 3])
      .then(() => conn.select('upstest', 'primary', 1, 0, 'eq', 1))
      .then((tuples) => {
        assert.equal(tuples.length, 1);
        assert.deepEqual(tuples[0], [1, 2, 3]);
        done();
      })

      .catch((e) => {
        done(e);
      });
  });
  it('update', (done) => {
    conn.upsert('upstest', [['+', 2, 2]], [2, 4, 3])
      .then(() => conn.upsert('upstest', [['+', 2, 2]], [2, 4, 3]))
      .then(() => conn.select('upstest', 'primary', 1, 0, 'eq', 2))
      .then((tuples) => {
        assert.equal(tuples.length, 1);
        assert.deepEqual(tuples[0], [2, 4, 5]);
        done();
      })

      .catch((e) => {
        done(e);
      });
  });
});
describe('connection test with custom msgpack implementation', () => {
  let customConn;
  beforeEach(() => {
    customConn = TarantoolConnection(
      {
        port: 33013,
        msgpack: {
          encode(obj) {
            return mlite.encode(obj);
          },
          decode(buf) {
            return mlite.decode(buf);
          },
        },
        lazyConnect: true,
        username: 'test',
        password: 'test',
      },
    );
  });
  it('connect', (done) => {
    customConn.connect().then(() => {
      done();
    }, (e) => { throw 'not connected'; });
  });
  it('should be authenticated', (done) => {
    conn.eval('return box.session.user()')
      .then((res) => {
        assert.equal(res[0], 'test');
        done();
      })
      .catch((e) => { done(e); });
  });
});
