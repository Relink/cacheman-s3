"use strict";

var proxyquire = require('proxyquire');
var chai = require('chai');
chai.use(require('sinon-chai'));
var expect = chai.expect;
var assert = require("assert");
var Mocha = require("mocha");
var sinon = require('sinon');
var describe = Mocha.describe;
var before = Mocha.before;
var after = Mocha.after;
var it = Mocha.it;

var cache;


describe("cacheman-s3", function () {
  var cache;
  var S3Mock;


  beforeEach(function () {
    S3Mock = {
      getBucket: sinon.stub(),
      putObject: sinon.stub(),
      listObjects: sinon.stub(),
      getObject: sinon.stub(),
      headObject: sinon.stub(),
      deleteObject: sinon.stub(),
      deleteObjects: sinon.stub()
    }

    var Cache = proxyquire("./index", {
      "aws-sdk": {
        S3: function () { return S3Mock }
      }
    });

    cache = new Cache({
      bucket: "cacheman",
      endpoint: "endpoint"
    });
  });

  it("should have main methods", function () {
    assert.ok(cache.set);
    assert.ok(cache.get);
    assert.ok(cache.del);
    assert.ok(cache.clear);
  });

  it("should store json", function () {
    cache.set("test1", { a: 1 })
    var parsable = S3Mock.putObject.firstCall.args[0].Body;
    expect(JSON.parse(parsable)).to.deep.equal( { a: 1 })
  });

  it("should store zero", function () {
    cache.set("test", 0)
    expect(S3Mock.putObject).to.have.been.calledWith(
      { Key:"test", Bucket: "cacheman", Body: "0" }
    )
  });

  it("should store false", function () {
    cache.set("test", false)
    expect(S3Mock.putObject).to.have.been.calledWith(
      { Key:"test", Bucket: "cacheman", Body: "false" }
    )
  });

  it("should store null", function () {
    cache.set("test", null)
    expect(S3Mock.putObject).to.have.been.calledWith(
      { Key:"test", Bucket: "cacheman", Body: "null" }
    )
  });

  it("should delete items", function () {
    cache.del("foo");
    expect(S3Mock.deleteObject).to.have.been.calledWith({
      Bucket: "cacheman",
      Key: "foo"
    });
  });

  it("should list objects with prefix when deleting glob-style patterns", function () {
    cache.del("foo/bar_baz*");
    var pre = S3Mock.listObjects.firstCall.args[0].Prefix;
    expect(pre).to.equal("foo/bar_baz")
  })

  it("should delete items with glob-style patterns", function () {
    S3Mock.listObjects.callsArgWith(1, null, { Contents: [ {Key: "foo1"}, {Key: "foo2" }]})
    cache.del("foo*");
    expect(S3Mock.deleteObjects).to.have.been.calledWith({
      Bucket: "cacheman",
      Delete: { Objects: [{Key: "foo1"}, {Key: "foo2"}]}
    });
  });

  it("should set Expiry to the current time plus the seconds added", function () {
    cache.set("test7", { a: 1 }, 10);
    var e = S3Mock.putObject.firstCall.args[0].Expires;
    expect(new Date(e).getTime() > Date.now())
  });

  it("should not return an expired key", function (done) {
    var expires = new Date(Date.now() - 2000).toISOString();
    S3Mock.headObject.callsArgWith(1, null, { Expires: expires });
    cache.get("foo", function (err, data) {
      expect(S3Mock.getObject).to.not.have.been.called;
      expect(data).to.equal(null)
      done();
    });
  });

  it("should return null when asked for a key modified before its fromDate", function (done) {
    var fromDate = Date.now();
    var modifiedDate = new Date(fromDate - 1000).toISOString();
    S3Mock.headObject.callsArgWith(1, null, { LastModified: modifiedDate });
    cache.get("foo", { fromDate: fromDate }, function (err, data) {
      expect(data).to.equal(null);
      done();
    })
  });

  it("should return an expired key if fromDate is fresh enough", function (done) {
    var fromDate = Date.now() - 1000;
    var modifiedDate = new Date(fromDate + 1000).toISOString();
    var expires = new Date(fromDate - 2000).toISOString();

    S3Mock.headObject.callsArgWith(1, null, { Expires: expires, LastModified: modifiedDate });
    S3Mock.getObject.callsArgWith(1, null, { Body: JSON.stringify({ foo: "bar" }) });
    cache.get("foo", { fromDate: fromDate }, function (err, data) {
      expect(S3Mock.getObject).to.have.been.calledWith({ Bucket: "cacheman", Key: "foo" });
      expect(data).to.deep.equal({ foo: "bar" });
      done();
    });
  });

  it("should clear an empty cache", function () {
    S3Mock.listObjects.callsArgWith(1, null, { Contents: [ {Key: "foo1"}]})
    cache.clear();

    expect(S3Mock.listObjects).to.have.been.calledWith({ Bucket: "cacheman", Prefix: "" })
    expect(S3Mock.deleteObjects).to.have.been.calledWith({
      Bucket: "cacheman",
      Delete: { Objects: [{Key: "foo1"}]}
    });
  });
});
