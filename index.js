'use strict';

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');
var AWS = require('aws-sdk');
var debug = require('debug')('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    'handleMessage'
  ];
var SQS_BATCH_LIMIT = 10;

/**
 * Construct a new SQSError
 */
function SQSError(message) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = (message || '');
}
util.inherits(SQSError, Error);

function validate(options) {
  requiredOptions.forEach(function (option) {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });

  if (options.batchSize < 1) {
    throw new Error('SQS batchSize option must be greater than 0.');
  }
}

function isAuthenticationError(err) {
  return (err.statusCode === 403 || err.code === 'CredentialsError');
}

/**
 * An SQS consumer.
 * @param {object} options
 * @param {string} options.queueUrl
 * @param {string} options.region
 * @param {function} options.handleMessage
 * @param {array} options.attributeNames
 * @param {array} options.messageAttributeNames
 * @param {number} options.batchSize
 * @param {object} options.sqs
 * @param {number} options.visibilityTimeout
 * @param {number} options.waitTimeSeconds
 */
function Consumer(options) {
  validate(options);

  this.queueUrl = options.queueUrl;
  this.handleMessage = options.handleMessage;
  this.attributeNames = options.attributeNames || [];
  this.messageAttributeNames = options.messageAttributeNames || [];
  this.stopped = true;
  this.batchSize = options.batchSize || 1;
  this.callEachBatchItem = options.callEachBatchItem !== false;
  this.visibilityTimeout = options.visibilityTimeout;
  this.waitTimeSeconds = options.waitTimeSeconds || 20;
  this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;

  this.sqs = options.sqs || new AWS.SQS({
    region: options.region || 'eu-west-1'
  });

  this._processMessageBound = this._processMessage.bind(this);
  this._deleteMessagePromiseBound = this._deleteMessagePromise.bind(this);
}

util.inherits(Consumer, EventEmitter);

/**
 * Construct a new Consumer
 */
Consumer.create = function (options) {
  return new Consumer(options);
};

/**
 * Start polling for messages.
 */
Consumer.prototype.start = function () {
  if (this.stopped) {
    debug('Starting consumer');
    this.stopped = false;
    this._poll();
  }
};

/**
 * Stop polling for messages.
 */
Consumer.prototype.stop = function () {
  debug('Stopping consumer');
  this.stopped = true;
};

Consumer.prototype._pollPromise = function (receiveParams) {
  var consumer = this;
  return new Promise(function(resolve, reject){
    consumer.sqs.receiveMessage(receiveParams, function(err, response){
      if(err){
        return reject(err);
      }
      resolve(response);
    });
  });
};

Consumer.prototype._paginatedPoll = function (receiveParams, remaining) {
  var consumer = this;
  var response;
  if(remaining === null || remaining === undefined){
    remaining = 0;
  }

  return consumer._pollPromise(receiveParams)
    .then(function(_response){
      response = _response;
      remaining = remaining - receiveParams.MaxNumberOfMessages;
      if (!(_response && _response.Messages && _response.Messages.length === SQS_BATCH_LIMIT && consumer.batchSize > SQS_BATCH_LIMIT)) {
        return;
      }
      if(remaining < 1){
        return;
      }
      receiveParams.WaitTimeSeconds = consumer.waitTimeSeconds;
      receiveParams.MaxNumberOfMessages = remaining > SQS_BATCH_LIMIT ? SQS_BATCH_LIMIT : remaining;
      return consumer._paginatedPoll(receiveParams, remaining);
    })
    .then(function(_response){
      if(_response && _response.Messages && Array.isArray(_response.Messages)){
        response.Messages = response.Messages.concat(_response.Messages);
      }
      return response;
    });
};

Consumer.prototype._poll = function () {

  var consumer = this,
    paramsList = [],
    remaining = this.batchSize,
    currentMax = this.batchSize;

  while (remaining > 0){
     currentMax = remaining < SQS_BATCH_LIMIT ? remaining : SQS_BATCH_LIMIT;
    paramsList.push(this._paginatedPoll({
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: currentMax,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    }, currentMax));
    remaining = remaining - currentMax;
  }
  if (!this.stopped) {
    debug('Polling for messages');
    Promise.all(paramsList)
      .then(function (responseList) {
        debug('Polled messages');
        var response = responseList.shift();
        responseList.forEach(function(_response){
          if(_response && _response.Messages){
            response.Messages = response.Messages.concat(_response.Messages);
          }
        });
        return consumer._handleSqsResponse(null, response);
      })
      .catch(function (err) {
        debug('Error polling messages', err);
        return consumer._handleSqsResponse(err);
      });

  } else {
    this.emit('stopped');
  }
};

Consumer.prototype._handleSqsResponse = function (err, response) {
  var consumer = this;

  if (err) {
    this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
  }

  debug('Received SQS response');
  //debug(response);

  var messages = [];

  if (response && response.Messages && response.Messages.length > 0) {
    messages = response.Messages;
    if(!consumer.callEachBatchItem) {
      messages = [messages];
    }

    async.each(messages, this._processMessageBound, function () {
      // start polling again once all of the messages have been processed
      consumer._poll();
    });
  } else if (response && !response.Messages) {
    this.emit('empty');
    this._poll();
  } else if (err && isAuthenticationError(err)) {
    // there was an authentication error, so wait a bit before repolling
    debug('There was an authentication error. Pausing before retrying.');
    setTimeout(this._poll.bind(this), this.authenticationErrorTimeout);
  } else {
    // there were no messages, so start polling again
    this._poll();
  }
};

Consumer.prototype._processMessage = function (message, cb) {
  var consumer = this;
  this.emit('message_received', message);
  async.series([
    function handleMessage(done) {
      consumer.handleMessage(message, done);
    },
    function deleteMessage(done) {
      consumer._deleteMessage(message, done);
    }
  ], function (err) {
    if (err) {
      if (err.name === SQSError.name) {
        consumer.emit('error', err);
      } else {
        consumer.emit('processing_error', err);
      }
    } else {
      consumer.emit('message_processed', message);
    }
    cb();
  });
};

Consumer.prototype._deleteMessagePromise = function (deleteParams, cb) {
  this.sqs.deleteMessageBatch(deleteParams, function (err, response) {
      if (err) return cb(new SQSError('SQS delete message failed: ' + err.message));
      cb(response);
  });
};

Consumer.prototype._deleteMessage = function (_messages, cb) {
  var consumer = this,
    ids = [],
    messages = [];
  
  if(!Array.isArray(_messages)){
    _messages = [_messages];
  }

  while (_messages.length > 0) {
    messages.push(_messages.splice(0, SQS_BATCH_LIMIT));
  }

  messages = messages.map(function(group){
    return {
      QueueUrl: consumer.queueUrl,
      Entries: group.map(function (item, i){
        ids.push(item.MessageId);
        return {
          Id: i + '',
          ReceiptHandle: item.ReceiptHandle
        };
      })
    };
  });
  debug('Deleting messages %d %s', ids.length, ids.join(', '));
  async.each(messages, consumer._deleteMessagePromiseBound, cb);
};

module.exports = Consumer;
