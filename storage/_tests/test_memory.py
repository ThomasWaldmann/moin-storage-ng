# Copyright: 2011 MoinMoin:ThomasWaldmannn
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memory storage tests
"""


from __future__ import absolute_import, division

from storage.memory import Storage
from storage._tests import StorageTestBase


class TestStorage(StorageTestBase):
    def setup_method(self, method):
        self.st = Storage()
        self.st.create()
        self.st.open()

