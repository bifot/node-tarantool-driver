const msgpack = require('msgpack-lite');
const debug = require('debug')('tarantool-driver:response');
const tarantoolConstants = require('./const');
const utils = require('./utils');

const Decoder = msgpack.Decoder;
const decoder = new Decoder();

exports._processResponse = function (buffer, offset, length) {
  offset = offset || 0;
  decoder.buffer = buffer;
  decoder.offset = offset + 23;
  const obj = decoder.fetch();
  const schemaId = buffer.readUInt32BE(offset + 19);
  const reqId = buffer.readUInt32BE(offset + 13);
  const code = buffer.readUInt32BE(offset + 3);
  if (this.schemaId) {
    if (this.schemaId != schemaId) {
      this.schemaId = schemaId;
      this.namespace = {};
    }
  } else {
    this.schemaId = schemaId;
  }
  let task;
  for (let i = 0; i < this.commandsQueue.length; i++) {
    task = this.commandsQueue._list[(this.commandsQueue._head + i) & this.commandsQueue._capacityMask];
    if (task[1] == reqId) {
      this.commandsQueue.removeOne(i);
      break;
    }
  }
  const dfd = task[2];
  const success = code == 0;
  if (success) dfd.resolve(_returnBool(task[0], obj[tarantoolConstants.KeysCode.data]));
  else dfd.reject(new utils.TarantoolError(obj[tarantoolConstants.KeysCode.error]));
};

exports._addToInnerBuffer = function (data, from, size) {
  const multiplierBuffer = 2;
  let newBuffer;
  let destLen;
  let newLen;
  if (typeof from !== 'undefined' && typeof size !== 'undefined') {
    if (this.bufferOffset + this.bufferLength + size < this.bufferSlide.length) {
      data.copy(this.bufferSlide, this.bufferOffset + this.bufferLength, from, from + size);
      this.bufferLength += size;
    } else {
      destLen = size + this.bufferLength;
      if (this.bufferSlide.length > destLen) {
        newBuffer = utils.createBuffer(this.bufferSlide.length * multiplierBuffer);
        this.bufferSlide.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength);
        data.copy(newBuffer, this.bufferLength, from, from + size);
        this.bufferSlide = newBuffer;
      } else {
        newLen = this.bufferSlide.length * multiplierBuffer;
        while (newLen < destLen) newLen *= multiplierBuffer;
        newBuffer = utils.createBuffer(newLen);
        this.bufferSlide.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength);
        data.copy(newBuffer, this.bufferLength, from, from + size);
        this.bufferSlide = newBuffer;
      }
      this.bufferOffset = 0;
      this.bufferLength = destLen;
    }
  } else if (this.bufferOffset + this.bufferLength + data.length < this.bufferSlide.length) {
    data.copy(this.bufferSlide, this.bufferOffset + this.bufferLength);
    this.bufferLength += data.length;
  } else {
    destLen = data.length + this.bufferLength;
    if (this.bufferSlide.length > destLen) {
      newBuffer = utils.createBuffer(this.bufferSlide.length * multiplierBuffer);
      this.bufferSlide.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength);
      data.copy(newBuffer, this.bufferLength);
      this.bufferSlide = newBuffer;
    } else {
      newLen = this.bufferSlide.length * multiplierBuffer;
      while (newLen < destLen) newLen *= multiplierBuffer;
      newBuffer = utils.createBuffer(newLen);
      this.bufferSlide.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength);
      data.copy(newBuffer, this.bufferLength);
      this.bufferSlide = newBuffer;
    }
    this.bufferOffset = 0;
    this.bufferLength = destLen;
  }
};

function _returnBool(cmd, data) {
  switch (cmd) {
    case tarantoolConstants.RequestCode.rqAuth:
    case tarantoolConstants.RequestCode.rqPing:
      return true;
    default:
      return data;
  }
}
