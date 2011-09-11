# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - fs storage tests
"""


from __future__ import absolute_import, division

import os, tempfile

from storage.fs import Storage
from storage._tests import StorageTestBase


class TestStorage(StorageTestBase):
    def setup_method(self, method):
        path = tempfile.mkdtemp()
        os.rmdir(path)
        self.st = Storage(path)
        self.st.create()
        self.st.open()

