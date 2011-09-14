# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - sqlite storage tests
"""


from __future__ import absolute_import, division

from storage.sqlite import BytesStorage, FileStorage
from storage._tests import BytesStorageTestBase, FileStorageTestBase


class TestBytesStorage(BytesStorageTestBase):
    def setup_method(self, method):
        self.st = BytesStorage('testdb.sqlite', 'testbs', compression_level=0) # ':memory:' does not work, strange
        self.st.create()
        self.st.open()


class TestBytesStorageCompressed(BytesStorageTestBase):
    def setup_method(self, method):
        self.st = BytesStorage('testdb.sqlite', 'testbs', compression_level=1) # ':memory:' does not work, strange
        self.st.create()
        self.st.open()


class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage('testdb.sqlite', 'testfs', compression_level=0) # ':memory:' does not work, strange
        self.st.create()
        self.st.open()


class TestFileStorageCompressed(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage('testdb.sqlite', 'testfs', compression_level=1) # ':memory:' does not work, strange
        self.st.create()
        self.st.open()

