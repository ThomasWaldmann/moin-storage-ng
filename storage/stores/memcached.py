# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memcached "store" (rather a CACHE, non-persistent, in RAM)

Note: does not support iteration.
"""


from __future__ import absolute_import, division

from StringIO import StringIO

import memcache

from . import MutableStoreBase, BytesMutableStoreBase, FileMutableStoreBase


class _Store(MutableStoreBase):
    """
    A simple dict-based in-memory store. No persistence!
    """
    def __init__(self, servers=['localhost:11211'], debug=0):
        """
        :param servers: list of memcached servers (default: ['localhost:11211'])
        :param debug: debug parameter given to memcache.Client (default: 0)
        """
        self.servers = servers
        self.debug = debug

    def create(self):
        pass

    def destroy(self):
        pass

    def open(self):
        self._mc = memcache.Client(self.servers, debug=self.debug)

    def close(self):
        self._mc.disconnect_all()

    def __iter__(self):
        # memcached does not support this
        return iter([])

    def __delitem__(self, key):
        self._mc.delete(key)


class BytesStore(_Store, BytesMutableStoreBase):
    def __getitem__(self, key):
        value = self._mc.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self._mc.set(key, value)


class FileStore(_Store, FileMutableStoreBase):
    def __getitem__(self, key):
        value = self._mc.get(key)
        if value is None:
            raise KeyError(key)
        return StringIO(value)

    def __setitem__(self, key, stream):
        self._mc.set(key, stream.read())

