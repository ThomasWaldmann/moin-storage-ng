# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - kyoto cabinet storage
"""


from __future__ import absolute_import, division

from StringIO import StringIO

from kyotocabinet import *

from storage import MutableStorageBase, BytesMutableStorageBase, FileMutableStorageBase

class _Storage(MutableStorageBase):
    """
    A simple dict-based in-memory storage. No persistence!
    """
    def __init__(self, filename):
        self.filename = filename

    def create(self):
        pass

    def destroy(self):
        self.open()
        self._db.clear()
        self.close()

    def open(self):
        self._db = DB()
        if not self._db.open(self.filename, DB.OWRITER | DB.OCREATE):
            raise IOError("open error: " + str(self._db.error()))

    def close(self):
        if not self._db.close():
            raise IOError("close error: " + str(self._db.error()))

    def __len__(self):
        return len(self._db)

    def __iter__(self):
        return iter(self._db)

    def __delitem__(self, key):
        self._db.remove(key)


class BytesStorage(_Storage, BytesMutableStorageBase):
    def __getitem__(self, key):
        value = self._db.get(key)
        if value is None:
            raise KeyError("get error: " + str(self._db.error()))
        return value

    def __setitem__(self, key, value):
        if not self._db.set(key, value):
            raise KeyError("set error: " + str(self._db.error()))


class FileStorage(_Storage, FileMutableStorageBase):
    def __getitem__(self, key):
        value = self._db.get(key)
        if value is None:
            raise KeyError("get error: " + str(self._db.error()))
        return StringIO(value)

    def __setitem__(self, key, stream):
        if not self._db.set(key, stream.read()):
            raise KeyError("set error: " + str(self._db.error()))

