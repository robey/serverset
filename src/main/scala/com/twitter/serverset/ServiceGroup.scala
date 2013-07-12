package com.twitter.serverset

/**
 * TODO:
 * - authentication
 */

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util.{Future, JavaTimer, Promise, Timer}
import com.twitter.zk.{RetryPolicy, ZkClient, ZNode}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import scala.annotation.tailrec
import scala.collection.mutable

object ServiceGroup {
  final val DefaultAcl = new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE)

  /**
   * A host/port pair.
   */
  case class Endpoint(host: String, port: Int) {
    /**
     * Build a thrift object out of an endpoint.
     */
    def toThrift: thrift.Endpoint = thrift.Endpoint(host, port)
  }

  object Endpoint {
    /**
     * Create an Endpoint out of an InetSocketAddress, pulling the host/port info out for you.
     * Normally you would pass in a listening socket that has already been bound to a port.
     * If the address is bound to the "wildcard" address (any interface is acceptable), we'll ask
     * java what it thinks the canonical IP should be.
     */
    def apply(socketAddress: InetSocketAddress): Endpoint = {
      val address = if (socketAddress.getAddress.isAnyLocalAddress) {
        // this is just the wildcard address. :(
        // this happens if a server is listening on all interfaces and java doesn't care.
        // so: just pick the "canonical" hostname.
        InetAddress.getLocalHost
      } else {
        socketAddress.getAddress
      }
      Endpoint(address.getHostAddress, socketAddress.getPort)
    }

    /**
     * Equivalent to `apply(socketAddress.asInstanceOf[InetSocketAddress])`, for finagle users.
     */
    def apply(socketAddress: SocketAddress): Endpoint = {
      apply(socketAddress.asInstanceOf[InetSocketAddress])
    }

    /**
     * Build an endpoint out of a thrift object.
     */
    def apply(x: thrift.Endpoint): Endpoint = Endpoint(x.host, x.port)
  }

  type Status = thrift.Status
  val Status = thrift.Status

  /**
   * A member of the service group, including a list of network addresses the member is listening
   * on.
   */
  case class Member(
    endpoint: Endpoint,
    endpoints: Map[String, Endpoint],
    status: Status
  ) {
    /**
     * Build a thrift object out of a member.
     */
    def toThrift: thrift.ServiceInstance = {
      thrift.ServiceInstance(endpoint.toThrift, endpoints.mapValues(_.toThrift), status)
    }
  }

  object Member {
    def apply(endpoint: Endpoint): Member = Member(endpoint, Map(), Status.Alive)

    /**
     * Build a member out of a thrift object.
     */
    def apply(x: thrift.ServiceInstance): Member = {
      Member(Endpoint(x.serviceEndpoint), x.additionalEndpoints.mapValues(Endpoint(_)).toMap, x.status)
    }
  }

  private[serverset] def mkdirs(node: ZNode): Future[ZNode] = {
    node.create(acls = Seq(DefaultAcl), mode = CreateMode.PERSISTENT) rescue {
      case e: KeeperException if e.code == KeeperException.Code.NODEEXISTS => {
        // good.
        Future(node)
      }
      case e: KeeperException if e.code == KeeperException.Code.NONODE => {
        // walk one level up the tree and try again.
        mkdirs(node.parent) flatMap { _ => mkdirs(node) }
      }
    }
  }

  /**
   * Create a new `ServiceGroup` from a zookeeper host list and a zookeeper path name. The client
   * will have a default connect timeout and session timeout. If the group doesn't exist yet on
   * the server, its path will be created asynchronously.
   *
   * The `hostList` follows apache-zookeeper's standard, so it's a comma-separated list of
   * `host:port` pairs.
   */
  def apply(
    hostList: String,
    name: Name,
    codec: ServiceInstanceCodec = DefaultCodec
  ): ServiceGroup = {
    implicit val timer = new JavaTimer(true)
    apply(
      ZkClient(hostList, Some(5.seconds), 1.minute).withRetryPolicy(RetryPolicy.Exponential(100.milliseconds)),
      name,
      codec
    )
  }

  /**
   * Create a new `ServiceGroup` from a zookeeper client and a zookeeper path name. If the group
   * doesn't exist yet on the server, its path will be created asynchronously.
   */
  def apply(
    zookeeper: ZkClient,
    name: Name,
    codec: ServiceInstanceCodec
  ): ServiceGroup = {
    new ServiceGroup(zookeeper(name.toPath), codec)
  }

  /**
   * A group name specification, which is used to determine the zookeeper path name for a group.
   */
  case class Name(group: String, service: String, environment: String = "production") {
    def toPath = "/twitter/services/%s/%s/%s".format(group, service, environment)
  }

  /**
   * An update event for group membership, as reported by `monitor`.
   */
  case class Update(
    group: ServiceGroup,
    added: Set[Member],
    removed: Set[Member]
  )

  /**
   * A membership in a group, returned by `join`.
   */
  case class Membership(private val znode: ZNode, member: Member) {
    private[this] val log = Logger.get(getClass)

    /**
     * Cancel this group membership. The corresponding node will be removed from zookeeper.
     */
    def cancel(): Future[Unit] = znode.delete() map { _ => () }

    private[serverset] val cancelled = new Promise[Membership]

    /**
     * A future that will be triggered if/when this membership is cancelled or removed.
     */
    def onLeave: Future[Membership] = cancelled

    def ensureExistence() {
      znode.exists.watch() onSuccess { case ZNode.Watch(future, eventUpdate) =>
        future onSuccess { node =>
          eventUpdate onSuccess { _ => ensureExistence() }
        } onFailure { e =>
          log.debug("Evicted from server group %s", znode.path)
          cancelled.setValue(this)
        }
      }
    }
  }

  val JsonCodec = ServiceInstanceCodec.Json
  val ThriftCodec = ServiceInstanceCodec.Thrift

  /**
   * Which codec should be used by default (currently: thrift).
   */
  private[serverset] val DefaultCodec = ServiceInstanceCodec.Thrift
}

/**
 * A service group as represented in a zookeeper cluster.
 *
 * A service group is a (usually permanent) zookeeper node representing a service type -- for
 * example, "the prime number service". Under this node are ephemeral nodes, one for each active
 * instance of the service. Each ephemeral node has data attached to it, revealing which host &
 * port(s) the instance is running on.
 *
 * A group can be "joined" by attaching your `Member` record to the group. It can also be queried
 * for a snapshot of the current member list, or monitored for updates as they happen.
 *
 * For convenience, a `ServiceGroup` instance maintains an internal `Member` builder which you can
 * add endpoints to during server startup. When you're done, call the no-arg `join()` method to
 * have a `Member` object constructed for you automatically.
 */
class ServiceGroup(znode: ZNode, codec: ServiceInstanceCodec = ServiceGroup.DefaultCodec) {
  import ServiceGroup._

  private[this] val log = Logger.get(getClass)

  // keep a local Member "builder" going, so a server can register endpoints from scattered places
  private[this] val memberBuilder = new AtomicReference(Member(Endpoint("", 0)))

  private[this] def memberFromNode(node: ZNode): Future[Member] = {
    node.getData().map { data => Member(ServiceInstanceCodec.decode(data.bytes)) }
  }

  private[this] def mapSkipMissing[A, B](future: Future[A])(f: A => B): Future[Option[B]] = {
    future map { a =>
      Some(f(a))
    } rescue {
      case e: KeeperException if e.code == KeeperException.Code.NONODE => {
        // we lost a race: instance vanished before we could look up its info.
        Future(None)
      }
    }
  }

  /**
   * Return the zookeeper client connection for this group. The client connection can be used to
   * monitor session events.
   */
  def client: ZkClient = znode.client

  /**
   * Make sure the path for this service group actually exists in zookeeper, creating it if
   * necessary.
   */
  def ensureZookeeperPath(): Future[Unit] = {
    mkdirs(znode) map { _ => () }
  }

  /**
   * Monitor this group for changes. Each call returns a new, independent `Offer`.
   *
   * A sequence of `Update` notifications will be posted to the returned `Offer` as they
   * happen. Each notification contains this service group object, a set of added `Member`
   * objects, and a set of removed `Member` objects.
   *
   * On creating the monitor, the first event will report all current members as "added".
   *
   * If the connection to the zookeeper cluster is lost, updates will stop arriving. When/if the
   * cluster returns, updates will arrive again. If you care about timeliness, be sure to monitor
   * the connection status of the `ZkClient` to track disconnections. If a service instance is
   * added and removed (or the opposite) very quickly, you may not get notified of the blip.
   * Consider this a "coarse granularity" view of the group's current membership.
   *
   * The offer is usually fulfilled within the apache-zookeeper event callback thread, so you
   * should take care not to do anything time-consuming in your listener, or you may block other
   * zookeeper events. It's okay to start new zookeeper requests from within the event, because
   * they will be fired off asynchronously. You probably shouldn't sleep or compute pi, though.
   */
  def monitor(): Offer[Update] = {
    val broker = new Broker[Update]
    val cached = new mutable.HashMap[String, Member]()

    znode.monitorTree() foreach { case ZNode.TreeUpdate(parent, added, removed) =>
      // ignore notifications lower in the tree.
      if (parent == znode) {
        val addedInstances = Future.collect {
          added.toSeq.map { node =>
            mapSkipMissing(memberFromNode(node)) { instance =>
              cached(node.name) = instance
              instance
            }
          }
        }
        val removedInstances = removed.flatMap { node =>
          cached.remove(node.name)
        }.toSeq
        addedInstances onSuccess { a =>
          val addSet = a.flatten.toSet
          val removeSet = removedInstances.toSet
          log.debug("Group membership changed: %s (add: %d, remove: %d)", znode.path, addSet.size,
            removeSet.size)
          broker ! Update(this, addSet, removeSet)
        }
      }
    }
    broker.recv
  }

  @tailrec
  final private[this] def atomicModify[A](item: AtomicReference[A])(f: A => A) {
    val current = item.get()
    val update = f(current)
    if (!item.compareAndSet(current, update)) atomicModify(item)(f)
  }

  /**
   * Add an endpoint to the internal `Member` builder. This internal builder will be used to
   * construct a `Member` object when you call the no-arg version of `join()`.
   *
   * This call is thread-safe.
   */
  final def addEndpoint(name: String, endpoint: Endpoint) {
    atomicModify(memberBuilder) { member =>
      if (member eq null) {
        val e = new RuntimeException("Attempted to add an endpoint after server registration!")
        log.error(e, "Attempted to add an endpoint after server registration!")
        throw e
      }
      if (member.endpoint.port == 0) {
        member.copy(endpoint = endpoint)
      } else {
        member.copy(endpoints = member.endpoints + (name -> endpoint))
      }
    }
  }

  /**
   * Register a service instance with the group. This will create a unique "ephemeral" node in
   * zookeeper, which will automatically vanish shortly after this zookeeper client session ends.
   *
   * You may manually remove this instance from the group by calling `cancel()` on the returned
   * `Membership` object.
   */
  def join(member: Member): Future[Membership] = {
    znode("member_").create(
      data = codec.encode(member.toThrift),
      acls = Seq(DefaultAcl),
      mode = CreateMode.EPHEMERAL_SEQUENTIAL
    ) map { node =>
      log.debug("Joined server group %s", znode.path)
      val membership = Membership(node, member)
      membership.ensureExistence()
      membership
    }
  }

  /**
   * Register a service instance with the group, using the endpoints added via `addEndpoint` calls.
   * A new `Member` object is constructed out of the collected endpoints, and passed to
   * `join(Member)`.
   *
   * This method should only be called once, or you could end up in the same group multiple times.
   */
  def join(): Future[Membership] = {
    // stuffing "null" in will ensure a crash if you try to call addEndpoint again later.
    join(memberBuilder.getAndSet(null))
  }

  /**
   * Return a list of service instances that are currently a member of this group. No assurance is
   * made that the information is still correct by the time it's processed -- use `monitor()` to
   * get live updates of group membership.
   */
  def list(): Future[Seq[Member]] = {
    znode.getChildren().map { _.children }.flatMap { nodes =>
      Future.collect {
        nodes.map { node =>
          mapSkipMissing(memberFromNode(node)) { instance => instance }
        }
      }.map(_.flatten)
    } rescue {
      // it's ok for there to be no node. means there are no service instances. :)
      case e: KeeperException if e.code == KeeperException.Code.NONODE =>
        Future(Seq())
    }
  }
}
