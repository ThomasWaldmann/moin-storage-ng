# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memory storage (based on a dict)
"""


from __future__ import absolute_import, division

from StringIO import StringIO

from storage import MutableStorageBase, BytesMutableStorageBase, FileMutableStorageBase

class _Storage(MutableStorageBase):
    """
    A simple dict-based in-memory storage. No persistence!
    """
    def create(self):
        self._st = {}

    def destroy(self):
        self._st = None

    def __iter__(self):
        for key in self._st:
            yield key

    def __delitem__(self, key):
        del self._st[key]


class BytesStorage(_Storage, BytesMutableStorageBase):
    def __getitem__(self, key):
        return self._st[key]

    def __setitem__(self, key, value):
        self._st[key] = value


class FileStorage(_Storage, FileMutableStorageBase):
    def __getitem__(self, key):
        return StringIO(self._st[key])

    def __setitem__(self, key, stream):
        self._st[key] = stream.read()
