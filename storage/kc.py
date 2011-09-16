# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - kyoto cabinet storage

Note: only ONE process can open a kyoto cabinet in OWRITER (writable) mode.
"""


from __future__ import absolute_import, division

from StringIO import StringIO

from kyotocabinet import *

from storage import MutableStorageBase, BytesMutableStorageBase, FileMutableStorageBase

class _Storage(MutableStorageBase):
    """
    A simple dict-based in-memory storage. No persistence!
    """
    def __init__(self, path, mode=DB.OWRITER|DB.OAUTOTRAN, db_opts=DB.GCONCURRENT):
        """
        Store params for .open(). Please refer to kyotocabinet-python-legacy docs for more information.

        :param path: db path + options, examples:
                     "db.kch" - no compression, no encryption
                     "db.kch#zcomp=zlib" - ZLIB compression
                     "db.kch#zcomp=arc#zkey=yoursecretkey" - ARC4 encryption
                     "db.kch#zcomp=arcz#zkey=yoursecretkey" - ARC4 encryption, ZLIB compression
        :param mode: mode given to DB.open call (default: DB.OWRITER|DB.OAUTOTRAN)
        :param db_opts: opts given to DB(opts=...) constructor (default: DB.GCONCURRENT)
        """
        self.path = path
        self.mode = mode
        self.db_opts = db_opts

    def create(self):
        self.open(mode=self.mode|DB.OCREATE)
        self.close()

    def destroy(self):
        self.open(mode=self.mode|DB.OTRUNCATE)
        self.close()

    def open(self, mode=None):
        self._db = DB(self.db_opts)
        if mode is None:
            mode = self.mode
        if not self._db.open(self.path, mode):
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

