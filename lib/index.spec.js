"use strict";

var proxyquire = require('proxyquire');
var assert = require("assert");
var Mocha = require("mocha");
var describe = Mocha.describe;
var before = Mocha.before;
var after = Mocha.after;
var it = Mocha.it;

var cache;

class S3Mock {
  constructor(options) {
    this.options = options;
    this.buckets = {};
  }

  getBucket(name) {
    if (!(name in this.buckets)) {
      this.buckets[name] = {};
    }
    return this.buckets[name];
  }

  putObject(params, cb) {
    var bucket = this.getBucket(params.Bucket);
    bucket[params.Key] = { Body: params.Body };
    if (params.Expires) {
      bucket[params.Key].Expires = params.Expires;
    }
    if (params.Metadata) {
      bucket[params.Key].Metadata = params.Metadata;
    }
    cb(null,bucket[params.Key]);
  }

  listObjects(params, cb) {
    var bucket = this.getBucket(params.Bucket);
    var result = [];
    var prefixLen = params.Prefix.length;
    var test = (x) => x.length >= prefixLen && x.substr(0, prefixLen) === params.Prefix;
    for (var existingKey in bucket) {
      if (test(existingKey)) result.push({Key: existingKey});
    }
    cb(null, { Contents: result });
  }

  getObject(params, cb) {
    var bucket = this.getBucket(params.Bucket);
    cb(null, bucket[params.Key]);
  }

  headObject(params, cb) {
    var bucket = this.getBucket(params.Bucket);
    if (params.Key in bucket) {
      cb(null, bucket[params.Key]);
    }
    else {
      cb({code: "NotFound"});
    }
  }

  deleteObject(params, cb) {
    var bucket = this.getBucket(params.Bucket);
    delete bucket[params.Key];
    cb(null,null);
  }

  deleteObjects(params, cb) {
    for(var idx = 0; idx < params.Delete.Objects.length; idx++) {
      this.deleteObject({
        Bucket  : params.Bucket,
        Key     : params.Delete.Objects[idx].Key
      }, () => {});
    }
    cb(null,null);
  }
}

var Cache = proxyquire("./index", {
  "aws-sdk": { S3: S3Mock }
});

describe("cacheman-s3", function () {

  before(function (done) {
    cache = new Cache({
      bucket: "cacheman",
      endpoint: "endpoint"
    });
    done();
  });

  after(function (done) {
    cache.clear();
    done();
  });

  it("should have endpoint configuration", function () {
    assert.equal(cache.client.options.endpoint,  "endpoint");
  });

  it("should have main methods", function () {
    assert.ok(cache.set);
    assert.ok(cache.get);
    assert.ok(cache.del);
    assert.ok(cache.clear);
  });

  it("should store items", function (done) {
    cache.set("test1", { a: 1 }, function (err) {
      if (err) return done(err);
      cache.get("test1", function (err, data) {
        if (err) return done(err);
        assert.equal(data.a, 1);
        done();
      });
    });
  });

  it("should store zero", function (done) {
    cache.set("test2", 0, function (err) {
      if (err) return done(err);
      cache.get("test2", function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, 0);
        done();
      });
    });
  });

  it("should store false", function (done) {
    cache.set("test3", false, function (err) {
      if (err) return done(err);
      cache.get("test3", function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, false);
        done();
      });
    });
  });

  it("should store null", function (done) {
    cache.set("test4", null, function (err) {
      if (err) return done(err);
      cache.get("test4", function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, null);
        done();
      });
    });
  });

  it("should delete items", function (done) {
    var value = Date.now();
    cache.set("test5", value, function (err) {
      if (err) return done(err);
      cache.get("test5", function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.del("test5", function (err) {
          if (err) return done(err);
          cache.get("test5", function (err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it("should delete items with glob-style patterns", function (done) {
    var value = Date.now();
    cache.set("foo_1", value, function (err) {
      if (err) return done(err);
      cache.set("foo_2", value, function (err) {
        if (err) return done(err);
        cache.get("foo_1", function (err, data) {
          if (err) return done(err);
          assert.equal(data, value);
          cache.del("foo*", function (err) {
            if (err) return done(err);
            cache.get("foo_1", function (err, data) {
              if (err) return done(err);
              assert.equal(data, null);
              cache.get("foo_2", function (err, data) {
                if (err) return done(err);
                assert.equal(data, null);
                done();
              });
            });
          });
        });
      });
    });
  });

  it("should clear items", function (done) {
    var value = Date.now();
    cache.set("test6", value, function (err) {
      if (err) return done(err);
      cache.get("test6", function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.clear(function (err) {
          if (err) return done(err);
          cache.get("test6", function (err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it("should expire key", function (done) {
    this.timeout(0);
    cache.set("test7", { a: 1 }, 1, function (err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get("test7", function (err, data) {
          if (err) return done(err);
          assert.equal(data, null);
          done();
        });
      }, 1100);
    });
  });

  it("should expire keys older than fromDate", function (done) {
    this.timeout(0);
    var fromDate = new Date().getTime();
    cache.set("test7", { a: 1 }, { fromDate }, function (err) {
      if (err) return done(err);
      cache.get("test7", { fromDate: fromDate + 1 },  function (err, data) {
        if (err) return done(err);
        assert.equal(data, null);
        done();
      });
    });
  });

  it("should not expire key", function (done) {
    this.timeout(0);
    cache.set("test8", { a: 1 }, -1, function (err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get("test8", function (err, data) {
          if (err) return done(err);
          assert.deepEqual(data, { a: 1 });
          done();
        });
      }, 1000);
    });
  });

  it("should get the same value subsequently", function (done) {
    var val = "Test Value";
    cache.set("test", "Test Value", function () {
      cache.get("test", function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, val);
        cache.get("test", function (err, data) {
          if (err) return done(err);
          assert.strictEqual(data, val);
          cache.get("test", function (err, data) {
            if (err) return done(err);
            assert.strictEqual(data, val);
            done();
          });
        });
      });
    });
  });

  it("should clear an empty cache", function (done) {
    cache.clear(function (err) {
      if (err) throw err;
      done();
    });
  });

});
