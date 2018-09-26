/* global Promise */
const net = require('net');
const _ = require('lodash');

const utils = require('./utils');

function Connector(options) {
  this.options = options;
}

Connector.prototype.disconnect = function () {
  this.connecting = false;
  if (this.socket) {
    this.socket.end();
  }
};

Connector.prototype.connect = function (callback) {
  this.connecting = true;
  const connectionOptions = _.pick(this.options, ['port', 'host']);

  const _this = this;
  process.nextTick(() => {
    if (!_this.connecting) {
      callback(new utils.TarantoolError('Connection is closed.'));
      return;
    }
    let socket;
    try {
      socket = net.createConnection(connectionOptions);
    } catch (err) {
      callback(err);
      return;
    }
    _this.socket = socket;
    callback(null, socket);
  });
};

module.exports = Connector;
