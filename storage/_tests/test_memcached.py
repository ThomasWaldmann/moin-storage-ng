# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memcached "storage" (cache) tests

Note: you need to have a memcached running on localhost:11211 for the tests
      to succeed. If you don't, you'll see failures due to key errors.
"""


from __future__ import absolute_import, division

from storage.memcached import BytesStorage, FileStorage
from storage._tests import BytesStorageTestBase, FileStorageTestBase


class TestBytesStorage(BytesStorageTestBase):
    def setup_method(self, method):
        self.st = BytesStorage()
        self.st.create()
        self.st.open()

    def test_iter(self):
        """
        memcached does not support iteration
        """

    def test_len(self):
        """
        memcached does not support iteration
        """

class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage()
        self.st.create()
        self.st.open()

    def test_iter(self):
        """
        memcached does not support iteration
        """

    def test_len(self):
        """
        memcached does not support iteration
        """

