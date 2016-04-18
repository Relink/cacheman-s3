var AWS = require('aws-sdk');
var minimatch = require("minimatch");

/** 
 * put a cache item on S3
 * @param {string} key - Abide to S3 key rules, it's used directly
 * @param {object} data - Will be stringified
 * @param {number} [ttl] - in seconds
 * @param {function} fn - The callback that will receive an error or data about the result
 */
var set = function (key, data, ttl, fn) {
  if ('function' === typeof ttl) {
    fn = ttl;
    ttl = null;
  }

  var val;
  try {
    val = JSON.stringify(data);
  } catch (e) {
    return fn(e);
  }
  var setParams = {
    Bucket: this.bucket,
    Key: key,
    Body: val
  };

  if (ttl && ttl >= 0) {
    setParams.Expires = new Date(new Date().getTime() + ttl * 1000).toISOString();
  }

  this.client.putObject(setParams, function (err, data) {
    if (err) return fn(err);
    fn(null, data);
  });
};

/**
 * Obtains an object from cache
 * @param {string} key
 * @param {function} fn - the callback that will receive the object, already parsed; null for non-existing key or expired value
 */
var get = function (key, fn) {
  this.client.getObject({
    Bucket: this.bucket,
    Key: key
  }, function (err, data) {
    if (err) {
      if (err.code === "NoSuchKey") {
        return fn(null, null);
      }
      else {
        return fn(err);
      }
    }
    if (data.Expires) {
      if (new Date(data.Expires).getTime() < new Date().getTime()) {
        fn(null, null);
      }
    }
    return fn(null, JSON.parse(data.Body));
  });
};

/**
 * Deletes using Glob
 * @param {object} ctx - the 'this' called within
 * @param {string} key - actually a Globbed expression
 * @param {function} fn - receives error or result
 */
var delGlob = function (ctx, key, fn) {
  var prefixLen = key.indexOf('*');
  var prefix = key.substr(0, prefixLen);
  ctx.client.listObjects({
    Bucket: ctx.bucket,
    Prefix: prefix
  }, function (err, data) {
    var test = minimatch.makeRe(key);
    var toDelete = [];
    for (var contentKey in data.Contents) {
      var k = data.Contents[contentKey].Key;
      if (test.test(k)) {
        toDelete.push({ Key: k });
      }
    }
    if (toDelete.length > 0) {
      ctx.client.deleteObjects({
        Bucket: ctx.bucket,
        Delete: { Objects: toDelete }
      }, fn);
    }
    else {
      fn(null, null);
    }
  });
};

/**
 * Removes an item by key (also supports Glob)
 * @param {string} key - the key or an expression like 'foo*'
 * @param {function} fn - the callback to receive error or result
 */
var del = function (key, fn) {
  if (key.includes('*')) {
    delGlob(this, key, fn);
  }
  else {
    this.client.deleteObject({
      Bucket: this.bucket,
      Key: key
    }, function (err, data) {
      if (err) return fn(err);
      fn(null, data);
    });
  }
};

/**
 * Removes all items on this bucket
 * @param {function} fn - callback to receive errors if any
 */
var clear = function (fn) {
  delGlob(this, '*', fn);
};

var CachemanS3 = function (options) {
  options = options || {};
  options.bucket = options.bucket || "cacheman";
  var cli = new AWS.S3();
  cli.createBucket({ Bucket: options.bucket }, function () { });
  return {
    version: "1.0.1",
    bucket: options.bucket,
    client: cli,
    set: set,
    get: get,
    del: del,
    clear: clear
  };
};

module.exports = CachemanS3;
