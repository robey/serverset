package com.twitter.serverset

import com.codahale.jerkson.{Json => JJson}
import com.twitter.scrooge.{BinaryThriftStructSerializer, JsonThriftSerializer}
import org.apache.thrift.protocol.TField

/**
 * Interface for encoding/decoding a ServiceInstance object to/from thrift or json.
 */
trait ServiceInstanceCodec {
  def encode(instance: thrift.ServiceInstance): Array[Byte]
  def decode(data: Array[Byte]): thrift.ServiceInstance
}

// jerkson can't deal with scrooge's not-quite-case-classes
protected case class MaimedEndpoint(host: String, port: Int)
protected case class MaimedServiceInstance(
  serviceEndpoint: thrift.Endpoint,
  additionalEndpoints: Map[String, MaimedEndpoint],
  status: Int
) {
  def inflate = {
    thrift.ServiceInstance(
      serviceEndpoint = thrift.Endpoint(serviceEndpoint.host, serviceEndpoint.port),
      additionalEndpoints = additionalEndpoints mapValues { p =>
        thrift.Endpoint(p.host, p.port)
      },
      status = thrift.Status(status)
    )
  }
}

/**
 * Codec implementations.
 */
object ServiceInstanceCodec {
  private[this] val thriftSerializer = new BinaryThriftStructSerializer[thrift.ServiceInstance] {
    val codec = thrift.ServiceInstance
  }

  // funny story: the thrift serializer only encodes. does not decode.
  private[this] val jsonSerializer = new JsonThriftSerializer[thrift.ServiceInstance] {
    val codec = thrift.ServiceInstance
  }


  val Thrift = new ServiceInstanceCodec {
    def encode(instance: thrift.ServiceInstance) = thriftSerializer.toBytes(instance)
    def decode(data: Array[Byte]) = thriftSerializer.fromBytes(data)
  }

  val Json = new ServiceInstanceCodec {
    def encode(instance: thrift.ServiceInstance) = jsonSerializer.toBytes(instance)
    def decode(data: Array[Byte]) = JJson.parse[MaimedServiceInstance](data).inflate
  }

  def decode(data: Array[Byte]) = {
    if (data.size > 0 && data(0) == '{'.toByte) Json.decode(data) else Thrift.decode(data)
  }
}
