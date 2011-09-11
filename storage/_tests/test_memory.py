# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memory storage tests
"""


from __future__ import absolute_import, division

from storage.memory import BytesStorage, FileStorage
from storage._tests import BytesStorageTestBase, FileStorageTestBase


class TestBytesStorage(BytesStorageTestBase):
    def setup_method(self, method):
        self.st = BytesStorage()
        self.st.create()
        self.st.open()

class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage()
        self.st.create()
        self.st.open()


