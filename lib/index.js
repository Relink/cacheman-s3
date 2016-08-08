var AWS = require("aws-sdk");
var minimatch = require("minimatch");
var noop = function() {};

/**
 * put a cache item on S3
 * @param {string} key - Abide to S3 key rules, it's used directly
 * @param {object} data - Will be stringified
 * @param {number} [ttl] - in seconds
 * @param {function} fn - The callback that will receive an error or data about the result
 */
var set = function (key, data, ttl, fn) {
  if ("function" === typeof ttl) {
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
 * @param {object} options - extra options parameters
 * @param {function} fn - the callback that will receive the object, already parsed; null for non-existing key or expired value
 */
var get = function (key, options, fn) {
  var options = options || {};
  var fromDate = options.fromDate;

  if ("function" === typeof options) {
    fn = options;
    options = {};
  }

  var ctx = this;
  ctx.client.headObject({
    Bucket: ctx.bucket,
    Key   : key
  }, function (err, data) {
    if (err) {
      if (err.code === "NotFound") {
        return fn(null, null);
      }
      else {
        return fn(err);
      }
    }

    // if there is a fromDate, we ignore expiry and compare modified time to the
    // fromDate, returning null if it's too old
    if (fromDate) {
      if (new Date(data.LastModified).getTime() < new Date(fromDate).getTime()) {
        return fn(null, null)
      }
    }

    // otherwise listen to TTL expiry and return null if it's expired
    else if (data.Expires && new Date(data.Expires).getTime() < Date.now()) {
      return fn(null, null);
    }

    ctx.client.getObject({
      Bucket  : ctx.bucket,
      Key     : key
    }, function(err,data) {
      if (err) return fn(err);
      return fn(null, JSON.parse(data.Body));
    });
  });
};


/**
 * Deletes using Glob
 * @param {object} ctx - the 'this' called within
 * @param {string} key - actually a Globbed expression
 * @param {function} fn - receives error or result
 */
var delGlob = function (ctx, key, fn) {
  if (!fn) fn = noop;
  var prefixLen = key.indexOf("*");
  var prefix = key.substr(0, prefixLen);
  ctx.client.listObjects({
    Bucket: ctx.bucket,
    Prefix: prefix
  }, function (err, data) {
    if (err) return fn(err)
    var test = minimatch.makeRe(key);
    var toDelete = data.Contents
          .filter(function (item) { return test.test(item.Key )})
          .map(function (item) { return { Key: item.Key } })

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
  if (!fn) fn = noop;
  if (key.includes("*")) {
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
  if (!fn) fn = noop;
  delGlob(this, "*", fn);
};

/**
 * Constructor
 * @param {{bucket: string, createS3Client: ?function}} options
 */
var CachemanS3 = function (options) {
  options = options || {};
  if (!options.bucket) throw new Error("Please specify the bucket to use on S3");
  var optn = options.endpoint ? { endpoint: options.endpoint } : undefined;
  var cli = new AWS.S3(optn);
  return {
    version: "1.1.1",
    bucket: options.bucket,
    client: cli,
    set: set,
    get: get,
    del: del,
    clear: clear
  };
};

module.exports = CachemanS3;
