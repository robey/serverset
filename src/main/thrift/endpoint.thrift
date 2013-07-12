namespace java com.twitter.serverset.thrift

/*
 * Represents the status of a service (deprecated).
 */
enum Status {
  ALIVE = 2
}

struct Endpoint {
  1: string host
  2: i32 port
}

struct ServiceInstance {
  1: Endpoint serviceEndpoint
  2: map<string, Endpoint> additionalEndpoints
  3: Status status;
}
