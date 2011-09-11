# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memory storage (based on a dict)
"""


from __future__ import absolute_import, division

from StringIO import StringIO

from storage import MutableStorageBase

class Storage(MutableStorageBase):
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

    def get_bytes(self, key):
        return self._st[key]

    def get_file(self, key):
        return StringIO(self._st[key])

    def set_bytes(self, key, value):
        self._st[key] = value

    def set_file(self, key, stream):
        value = stream.read()
        stream.close()
        self._st[key] = value

    def __delitem__(self, key):
        del self._st[key]

