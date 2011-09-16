# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - kyoto cabinet storage tests
"""


from __future__ import absolute_import, division

import pytest
pytest.importorskip('kyotocabinet')

from storage.kc import BytesStorage, FileStorage
from storage._tests import BytesStorageTestBase, FileStorageTestBase


class TestBytesStorage(BytesStorageTestBase):
    def setup_method(self, method):
        self.st = BytesStorage('testdb.kch') # *.kch
        self.st.create()
        self.st.open()

class TestFileStorage(FileStorageTestBase):
    def setup_method(self, method):
        self.st = FileStorage('testdb.kch') # *.kch
        self.st.create()
        self.st.open()

