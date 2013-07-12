serverset
=========

This was my final hack-week project at Twitter, in 2012: to implement a server
registration system using zookeeper, in a way simple enough that people would
actually use it. From the initial proposal:

For at least three years, we've dreamed of having a central database of live
servers, so that you could look up the list of active kestrel servers (for
example), and not have to maintain hard-coded server lists in config files.
This simple idea has hit so many roadblocks, some say it was built on a
Native American burial ground.

Over the years, various teams have tried to tackle this problem, in public
or in secret, and various platforms have been used. Many Bothans died in the
process. Eventually, a loose set of informal libraries coalesced around a
model that uses:

- zookeeper as the database
- thrift as a data format

This system is working, more or less, for several projects, and is the basic
standard as it exists on the ground.

My hack week project was to investigate the various libraries and tools being
used and built, figure out why many projects weren't using them yet, and try
to combine these existing tools into a very simple API that any project could
just "plug and play". As a secondary goal, I wanted to propose extensions or
migrations that would achieve any of the original goals that weren't being
met by the current implementations.

In the end, I had to write less than 250 lines of code to glue everything
together.

Does it work?
-------------

I don't promise that this works or even compiles. In particular, I remember
that I had to backport it to scala 2.8 at the last minute, because 2.9 wasn't
ready yet. Consider this reference code for implementing your own serverset-
like library.

Goals
-----

- combine the six existing libraries
- dead simple API
- few dependencies (apache-zookeeper + anything absolutely unavoidable)
- reuse existing code as much as possible
- compatible with what servers are currently doing (thrift)
- support for the standard (json)
- document what people are doing (and why), and our aspirations
- nodes should be immutable

Credit
------

Most of the code was based on conversations with Oliver Gould, Brian Wickman,
and Ben Hindman, describing how things worked, how things should work, and
showing me working or semi-working code in C++ and python.
