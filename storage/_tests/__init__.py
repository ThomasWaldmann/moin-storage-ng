# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - storage tests
"""


from __future__ import absolute_import, division

import pytest

from StringIO import StringIO


class _StorageTestBase(object):
    def setup_method(self, method):
        """
        self.st needs to be an created/opened storage
        """
        raise NotImplemented

    def teardown_method(self, method):
        """
        close and destroy self.st
        """
        self.st.close()
        self.st.destroy()

    def test_getitem_raises(self):
        with pytest.raises(KeyError):
            self.st['doesnotexist']


class FileStorageTestBase(_StorageTestBase):
    def test_setitem_getitem_delitem(self):
        k, v = 'key', 'value'
        self.st[k] = StringIO(v)
        assert v == self.st[k].read()
        del self.st[k]
        with pytest.raises(KeyError):
            self.st[k]

    def test_iter(self):
        kvs = set([('1', 'one'), ('2', 'two'), ('3', 'three'), ])
        for k, v in kvs:
            v = StringIO(v)
            self.st[k] = v
        result = set()
        for k in self.st:
            result.add((k, self.st[k].read()))
        assert result == kvs

    def test_len(self):
        assert len(self.st) == 0
        self.st['foo'] = StringIO('bar')
        assert len(self.st) == 1
        del self.st['foo']
        assert len(self.st) == 0

class BytesStorageTestBase(_StorageTestBase):
    def test_setitem_getitem_delitem(self):
        k, v = 'key', 'value'
        self.st[k] = v
        assert v == self.st[k]
        del self.st[k]
        with pytest.raises(KeyError):
            self.st[k]

    def test_iter(self):
        kvs = set([('1', 'one'), ('2', 'two'), ('3', 'three'), ])
        for k, v in kvs:
            self.st[k] = v
        result = set()
        for k in self.st:
            result.add((k, self.st[k]))
        assert result == kvs

    def test_len(self):
        assert len(self.st) == 0
        self.st['foo'] = 'bar'
        assert len(self.st) == 1
        del self.st['foo']
        assert len(self.st) == 0

