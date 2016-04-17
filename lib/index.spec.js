var assert = require( 'assert');
var Cache = require('../lib/index');

var cache;

describe('cacheman-s3', function() {

  before(function(done)  {
    cache = new Cache();
    done();
  });

  after(function(done) {
    cache.clear(done);
  });

  it('should have main methods', function() {
    assert.ok(cache.set);
    assert.ok(cache.get);
    assert.ok(cache.del);
    assert.ok(cache.clear);
  });

  it('should store items', function(done) {
    this.timeout(5000);
    cache.set('test1', { a: 1 }, function (err) {
      if (err) return done(err);
      cache.get('test1', function (err, data) {
        if (err) return done(err);
        assert.equal(data.a, 1);
        done();
      });
    });
  });

  it('should store zero', function (done) {
    this.timeout(5000);
    cache.set('test2', 0, function(err) {
      if (err) return done(err);
      cache.get('test2', function(err, data) {
        if (err) return done(err);
        assert.strictEqual(data, 0);
        done();
      });
    });
  });

  it('should store false', function(done) {
    this.timeout(5000);
    cache.set('test3', false, function(err) {
      if (err) return done(err);
      cache.get('test3', function(err, data) {
        if (err) return done(err);
        assert.strictEqual(data, false);
        done();
      });
    });
  });

  it('should store null', function(done) {
    this.timeout(5000);
    cache.set('test4', null, function(err) {
      if (err) return done(err);
      cache.get('test4', function(err, data) {
        if (err) return done(err);
        assert.strictEqual(data, null);
        done();
      });
    });
  });

  it('should delete items', function(done) {
    this.timeout(15000);
    var value = Date.now();
    cache.set('test5', value, function(err) {
      if (err) return done(err);
      cache.get('test5', function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.del('test5', function(err) {
          if (err) return done(err);
          cache.get('test5', function(err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it('should delete items with glob-style patterns', function(done) {
    this.timeout(20000);
    var value = Date.now();
    cache.set('foo_1', value, function(err)  {
      if (err) return done(err);
      cache.set('foo_2', value, function(err)  {
        if (err) return done(err);
        cache.get('foo_1', function(err, data)  {
          if (err) return done(err);
          assert.equal(data, value);
          cache.del('foo*', function(err)  {
            if (err) return done(err);
            cache.get('foo_1', function(err, data)  {
              if (err) return done(err);
              assert.equal(data, null);
              cache.get('foo_2', function(err, data)  {
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

  it('should clear items', function(done)  {
    this.timeout(10000);
    var value = Date.now();
    cache.set('test6', value, function(err) {
      if (err) return done(err);
      cache.get('test6', function(err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.clear(function(err) {
          if (err) return done(err);
          cache.get('test6', function(err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it('should expire key', function (done) {
    this.timeout(0);
    cache.set('test7', { a: 1 }, 1, function(err) {
      if (err) return done(err);
      setTimeout(function() {
        cache.get('test7', function(err, data) {
        if (err) return done(err);
          assert.equal(data, null);
          done();
        });
      }, 1100);
    });
  });

  it('should not expire key', function (done) {
    this.timeout(0);
    cache.set('test8', { a: 1 }, -1, function(err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get('test8', function (err, data) {
        if (err) return done(err);
          assert.deepEqual(data, { a: 1 });
          done();
        });
      }, 1000);
    });
  });

  it('should get the same value subsequently', function(done) {
    this.timeout(0);
    var val = 'Test Value';
    cache.set('test', 'Test Value', function() {
      cache.get('test', function(err, data) {
        if (err) return done(err);
        assert.strictEqual(data, val);
        cache.get('test', function(err, data) {
          if (err) return done(err);
          assert.strictEqual(data, val);
          cache.get('test', function(err, data) {
            if (err) return done(err);
             assert.strictEqual(data, val);
             done();
          });
        });
      });
    });
  });

  it('should clear an empty cache', function(done) {
    this.timeout(0);
    cache.clear(function(err, data) {
      done();
    });
  });

});