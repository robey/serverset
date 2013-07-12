package com.twitter.serverset

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.{Future, Promise, Return, Throw}
import com.twitter.zk.{ZNode, ZOp}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent}
import org.apache.zookeeper.data.ACL
import org.mockito.ArgumentMatcher
import org.mockito.Matchers._
import org.mockito.Matchers.{eq => eql}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

// these unit tests are okay, but the real proving ground is integration tests against a real zk.
class ServiceGroupSpec extends FunSpec with MockitoSugar {
  val instance1 = ServiceGroup.Member(ServiceGroup.Endpoint("host1", 69), Map(), ServiceGroup.Status.Alive)
  val instance2 = ServiceGroup.Member(ServiceGroup.Endpoint("host2", 69), Map(), ServiceGroup.Status.Alive)
  val instance3 = ServiceGroup.Member(ServiceGroup.Endpoint("host3", 69), Map(), ServiceGroup.Status.Alive)
  val NoNode = new KeeperException(KeeperException.Code.NONODE) { }

  def mockInstanceNode(member: ServiceGroup.Member, codec: ServiceInstanceCodec) = {
    val node = mock[ZNode]
    val op = mock[ZOp[ZNode.Data]]
    val data = mock[ZNode.Data]
    when(node.name) thenReturn member.endpoint.host
    when(node.getData) thenReturn op
    when(op.apply()) thenReturn Future(data)
    when(data.bytes) thenReturn codec.encode(member.toThrift)
    node
  }

  def mockMissingNode() = {
    val node = mock[ZNode]
    val dataOp = mock[ZOp[ZNode.Data]]
    val childrenOp = mock[ZOp[ZNode.Children]]
    when(node.getData) thenReturn dataOp
    when(node.getChildren) thenReturn childrenOp
    when(dataOp.apply()) thenReturn Future.exception(NoNode)
    when(childrenOp.apply()) thenReturn Future.exception(NoNode)
    node
  }

  def mockChildrenNodes(c: Seq[ZNode]): ZNode = {
    val node = mock[ZNode]
    val op = mock[ZOp[ZNode.Children]]
    val children = mock[ZNode.Children]
    when(node.getChildren) thenReturn op
    when(op.apply()) thenReturn Future(children)
    when(children.children) thenReturn c
    node
  }

  def mockChildren(c: Seq[ServiceGroup.Member], codec: ServiceInstanceCodec): ZNode = {
    mockChildrenNodes(c.map { i => mockInstanceNode(i, codec) })
  }

  def mockCreatableNode(
    parent: ZNode,
    member: ServiceGroup.Member,
    codec: ServiceInstanceCodec
  ): (ZNode, Promise[WatchedEvent]) = {
    val node = mock[ZNode.Exists]
    val op = mock[ZOp[ZNode.Exists]]
    val promise = new Promise[WatchedEvent]
    val watch1 = ZNode.Watch[ZNode.Exists](Return(node), promise)
    val watch2 = ZNode.Watch[ZNode.Exists](Throw(new Exception("boo")), promise)
    when(parent.apply("member_")) thenReturn parent
    val bytes = codec.encode(member.toThrift)
    when(parent.create(
      bytes,
      Seq(ServiceGroup.DefaultAcl),
      CreateMode.EPHEMERAL_SEQUENTIAL
    )) thenReturn Future(node)
    when(node.exists) thenReturn op
    when(op.watch()) thenReturn Future(watch1) thenReturn Future(watch2)
    (node, promise)
  }

  def forEachCodec(f: ServiceInstanceCodec => Unit) {
    List(ServiceInstanceCodec.Thrift, ServiceInstanceCodec.Json).foreach(f(_))
  }

  describe("ServiceGroup") {
    it("mkdirs") {
      val node1 = mock[ZNode]
      val node2 = mock[ZNode]
      val nodeFinal = mock[ZNode]
      when(node1.create(any[Array[Byte]], any[Seq[ACL]], any[CreateMode])) thenReturn
        Future.exception(NoNode) thenReturn Future(nodeFinal)
      when(node1.parent) thenReturn node2
      when(node2.create(any[Array[Byte]], any[Seq[ACL]], any[CreateMode])) thenReturn
        Future(node1)

      assert(ServiceGroup.mkdirs(node1).get() === nodeFinal)
    }

    describe("list") {
      it("with no members") {
        forEachCodec { codec =>
          val group = new ServiceGroup(mockMissingNode(), codec)
          assert(group.list.get() === Nil)
        }
      }

      it("with 2 members") {
        forEachCodec { codec =>
          val group = new ServiceGroup(mockChildren(Seq(instance1, instance2), codec), codec)
          assert(group.list.get() === Seq(instance1, instance2))
        }
      }

      it("with 3 members, one of which vanishes") {
        forEachCodec { codec =>
          val group = new ServiceGroup(mockChildrenNodes(Seq(
            mockInstanceNode(instance1, codec),
            mockMissingNode(),
            mockInstanceNode(instance3, codec)
          )), codec)
          assert(group.list.get() === Seq(instance1, instance3))
        }
      }
    }

    describe("join") {
      it("works") {
        forEachCodec { codec =>
          val parent = mock[ZNode]
          val (node, promise) = mockCreatableNode(parent, instance1, codec)
          val group = new ServiceGroup(parent, codec)
          assert(group.join(instance1).get() === ServiceGroup.Membership(node, instance1))
        }
      }

      it("notices being deregistered") {
        forEachCodec { codec =>
          val parent = mock[ZNode]
          val (node, promise) = mockCreatableNode(parent, instance1, codec)
          val group = new ServiceGroup(parent, codec)
          val membership = group.join(instance1).get()

          assert(!membership.onLeave.isDefined)
          promise.setValue(null)
          assert(membership.onLeave.isDefined)
        }
      }

      it("with builder") {
        forEachCodec { codec =>
          val parent = mock[ZNode]
          val group = new ServiceGroup(parent, codec)
          group.addEndpoint("fearsomud", ServiceGroup.Endpoint("10.0.0.1", 2000))
          group.addEndpoint("ssh", ServiceGroup.Endpoint("10.0.0.1", 22))

          val expectedMember = ServiceGroup.Member(
            ServiceGroup.Endpoint("10.0.0.1", 2000),
            Map("ssh" -> ServiceGroup.Endpoint("10.0.0.1", 22)),
            ServiceGroup.Status.Alive
          )
          val (node, promise) = mockCreatableNode(parent, expectedMember, codec)
          val membership = group.join().get()
          assert(membership === ServiceGroup.Membership(node, expectedMember))
        }
      }
    }

    it("leave") {
      val node = mock[ZNode]
      when(node.delete()) thenReturn Future(node)
      ServiceGroup.Membership(node, instance1).cancel().get()
    }

    describe("monitor") {
      it("works at all") {
        forEachCodec { codec =>
          val node = mock[ZNode]
          val broker = new Broker[ZNode.TreeUpdate]
          when(node.monitorTree()) thenReturn broker.recv
          val group = new ServiceGroup(node, codec)
          val monitor = group.monitor()

          broker ! ZNode.TreeUpdate(node, Set(), Set())
          assert(monitor.sync().get() === ServiceGroup.Update(group, Set(), Set()))
        }
      }

      it("fetches data about an addition") {
        forEachCodec { codec =>
          val node = mock[ZNode]
          val broker = new Broker[ZNode.TreeUpdate]
          when(node.monitorTree()) thenReturn broker.recv
          val group = new ServiceGroup(node, codec)
          val monitor = group.monitor()

          val node1 = mockInstanceNode(instance1, codec)
          broker ! ZNode.TreeUpdate(node, Set(node1), Set())
          assert(monitor.sync().get() === ServiceGroup.Update(group, Set(instance1), Set()))
        }
      }

      it("caches data about a removal") {
        forEachCodec { codec =>
          val node = mock[ZNode]
          val broker = new Broker[ZNode.TreeUpdate]
          when(node.monitorTree()) thenReturn broker.recv
          val group = new ServiceGroup(node, codec)
          val monitor = group.monitor()

          val node1 = mockInstanceNode(instance1, codec)
          broker ! ZNode.TreeUpdate(node, Set(node1), Set())
          assert(monitor.sync().get() === ServiceGroup.Update(group, Set(instance1), Set()))
          broker ! ZNode.TreeUpdate(node, Set(), Set(node1))
          assert(monitor.sync().get() === ServiceGroup.Update(group, Set(), Set(instance1)))
        }
      }

      it("doesn't panic if it misses a quick add/remove") {
        forEachCodec { codec =>
          val node = mock[ZNode]
          val broker = new Broker[ZNode.TreeUpdate]
          when(node.monitorTree()) thenReturn broker.recv
          val group = new ServiceGroup(node, codec)
          val monitor = group.monitor()

          val node1 = mockMissingNode()
          broker ! ZNode.TreeUpdate(node, Set(node1), Set())
          assert(monitor.sync().get() === ServiceGroup.Update(group, Set(), Set()))
        }
      }
    }
  }
}
