package com.twitter.serverset

import org.scalatest._

class ServiceInstanceCodecSpec extends FunSpec {
  val instance = thrift.ServiceInstance(
    thrift.Endpoint("main", 1),
    Map(
      "two" -> thrift.Endpoint("buffalo", 2),
      "three" -> thrift.Endpoint("trace", 3)
    ),
    thrift.Status.Alive
  )

  describe("ServiceInstanceCodec") {
    it("thrift") {
      val out = ServiceInstanceCodec.Thrift.decode(ServiceInstanceCodec.Thrift.encode(instance))
      assert(out === instance)
    }

    it("json") {
      val out = ServiceInstanceCodec.Json.decode(ServiceInstanceCodec.Json.encode(instance))
      assert(out === instance)
    }
  }
}
