# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - storage subsystem
============================

We use a layered approach like this::

 Indexing Middleware               does complex stuff like indexing, searching,
 |                                 listing, lookup by name, ACL checks, ...
 v
 Routing  Middleware               dispatches to multiple backends based on the
 |                 |               name, cares about absolute and relative names
 v                 v
 "stores" Backend  Other Backend   simple stuff: store, get, destroy revisions
 |           |
 v           v
 meta store  data store            simplest stuff: store, get, destroy and iterate
                                   over key/value pairs
"""

