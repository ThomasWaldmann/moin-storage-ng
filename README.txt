==========
storage-ng
==========
New storage system for moin2 wiki.

Currently this is living at:
https://bitbucket.org/thomaswaldmann/storage-ng/

It is intended as a incompatible replacement for the current stuff you see there:
https://bitbucket.org/thomaswaldmann/moin-2.0/src/tip/MoinMoin/storage/


Storage Layers
==============
We use a layered approach like this::

 Indexing Middleware   does complex stuff like indexing, searching, listing, ...
  |
  v
 Router Middleware     dispatches to multiple backends based on the name
  |          |
  v          v
 Backend1   Backend2   store, get, destroy revisions


Indexing Middleware
===================
This is the level that does complex stuff with the help of Whoosh (a fast
pure-Python indexing and search library).

Using Whoosh we build, maintain and use 2 indexes:

* "all revisions" index (big, needed for history search)
* "latest revisions" index (smaller, just the current revisions)

When creating or destroying revisions, indexes are automatically updated.

There is also code to do a full index rebuild in case it gets damaged, lost
or needs rebuilding for other reasons.

Indexing is the only layer that can easily deal with **names** (it can
easily translate names to UUIDs and vice versa) and with **items** (it
knows current revision, it can easily list and order historial revisions),
using the index.

The layers below are using UUIDs to identify revisions meta and data:

* revid (metaid) - a UUID identifying a specific revision (revision metadata)
* dataid - a UUID identifying some specific revision data (optional), it is
  just stored into revision metadata.
* itemid - a UUID identifying an item (== a set of revisions), it is just
  stored into revision metadata. itemid is only easily usable on indexing
  level.

Many methods provided by the indexing middleware will be fast, because they
will not access the layers below (like the storage), but just the index files,
usually it is even just the small and thus quick latest-revs index.


Router Middleware
=================
Just think of UNIX fstab and mount.

Lets you mount backends that store items belonging to some specific part
of the namespace. Router middleware has same API as a backend.


"storages" Backend
==================
This is a backend that ties together 2 key/value storages:

* meta storage
  - key = revid UUID (bytes, ascii)
  - value = bytes (bytes, utf-8)
* data storage
  - key = dataid UUID (bytes, ascii)
  - value = file (gets/returns open file instances, to read/write binary data)

Already implemented key/value storages:

* fs (stores into filesystem)
* memory (stores into RAM, non-persistent!)

Likely one can also use many other existing k/v stores with very little code.


"fileserver" Backend
====================
This is a read-only backend that exposes a part of the filesystem:

* files show as single revision items

  - metadata is made up from fs metadata + mimetype guessing
  - data is read from the file

* directories create a virtual directory item

