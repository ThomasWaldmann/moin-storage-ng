# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - storages backend tests

Note: theoretically, it should be enough to test with one kind of storage,
      but we better test with a fs AND a memory storage.
"""


from __future__ import absolute_import, division

import pytest

from backend.storages import MutableBackend
from backend._tests import BackendTestBase

from storage.memory import Storage as MemoryStorage

class TestMemoryBackend(BackendTestBase):
    def setup_method(self, method):
        meta_store = MemoryStorage()
        data_store = MemoryStorage()
        self.be = MutableBackend(meta_store, data_store)
        self.be.create()
        self.be.open()

import os, tempfile

from storage.fs import Storage as FSStorage

class TestFSBackend(BackendTestBase):
    def setup_method(self, method):
        meta_path = tempfile.mkdtemp()
        os.rmdir(meta_path)
        meta_store = FSStorage(meta_path)
        data_path = tempfile.mkdtemp()
        os.rmdir(data_path)
        data_store = FSStorage(data_path)
        self.be = MutableBackend(meta_store, data_store)
        self.be.create()
        self.be.open()


