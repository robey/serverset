
Proposed extensions to serverset
================================

Json support
------------

Embrace Attila's json encoding as standard: a direct mapping of field names
and types, with structs becoming json objects.

A service may register either itself using either json or thrift. When
reading registrations, the json entries can be idenitified by their first
byte: '{' (173). Thrift binary structures always begin with the "struct"
identifier byte: 12.

Data format extensions
----------------------

The current thrift struct lets you give a name to each host/port, but there
isn't really a standardized meaning for the names, and no indication of what
protocol they speak or what they're for.

Thrift "endpoints" should be marked as being thrift, and should refer to the
service API. Ideally they would uniquely identify a thrift service definition
in the thrift repo, so that you could use a serverset registration to reverse
engineer how to talk to the service.

Add two new fields to Endpoint:

    3: optional string serviceUri
    4: optional i64 memberId

The member ID is optional and reflects that this instance is somehow unique.
Service groups that use `memberId` must also have the member ID in their
zookeeper path (see below).

The service URI should follow a strict format, or not be present:

- HTTP URIs should indicate their purpose in the scheme, and may include a
  prefix path.
- Multiple endpoints may use the same host & port, if they're HTTP URIs with
  different prefix paths.
- Thrift URLs should include the exact name of the maven package containing
  their service description.

Examples:

    http+ostrich://10.0.0.1:9991/
    thrift://10.0.0.1:3993/Cuckoo-3

Desired zookeeper path
----------------------

    /twitter/services/(group)/(service)/(cluster)[/(member_id)]/(members)

Example:

    /twitter/services/kestrel/kestrel/production/(members)
