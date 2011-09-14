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
        self.st = BytesStorage('testdb.sqlite', 'testbs') # ':memory:' does not work, strange
        self.st.create()
        self.st.open()

class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage('testdb.sqlite', 'testfs') # ':memory:' does not work, strange
        self.st.create()
        self.st.open()


