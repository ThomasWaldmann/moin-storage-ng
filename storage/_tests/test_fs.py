# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - fs storage tests
"""


from __future__ import absolute_import, division

import os, tempfile

from storage.fs import BytesStorage, FileStorage
from storage._tests import BytesStorageTestBase, FileStorageTestBase

class TestBytesStorage(BytesStorageTestBase):
    def setup_method(self, method):
        path = tempfile.mkdtemp()
        os.rmdir(path)
        self.st = BytesStorage(path)
        self.st.create()
        self.st.open()

class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        path = tempfile.mkdtemp()
        os.rmdir(path)
        self.st = FileStorage(path)
        self.st.create()
        self.st.open()


