# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO

import pytest

class BackendTestBase(object):
    def setup_method(self, method):
        """
        self.be needs to be an opened backend
        """
        raise NotImplemented

    def teardown_method(self, method):
        """
        close self.be
        """
        self.be.close()

    def test_getrevision_raises(self):
        with pytest.raises(KeyError):
            self.be.get_revision('doesnotexist')

    def test_iter(self):
        assert list(self.be) == []


class MutableBackendTestBase(BackendTestBase):
    def setup_method(self, method):
        """
        self.be needs to be an created/opened backend
        """
        raise NotImplemented

    def teardown_method(self, method):
        """
        close and destroy self.be
        """
        self.be.close()
        self.be.destroy()

    def test_getrevision_raises(self):
        with pytest.raises(KeyError):
            self.be.get_revision('doesnotexist')

    def test_store_get_del(self):
        meta = dict(foo='bar')
        data = 'baz'
        metaid = self.be.store_revision(meta, StringIO(data))
        m, d = self.be.get_revision(metaid)
        assert m == meta
        assert d.read() == data
        self.be.del_revision(metaid)
        with pytest.raises(KeyError):
            self.be.get_revision(metaid)

    def test_iter(self):
        mds = [#(metadata items, data str)
                (dict(name='one'), 'ONE'),
                (dict(name='two'), 'TWO'),
                (dict(name='three'), 'THREE'),
              ]
        expected_result = set()
        for m, d in mds:
            k = self.be.store_revision(m, StringIO(d))
            # note: store_revision injects dataid and metaid into m
            m = tuple(sorted(m.items()))
            expected_result.add((k, m, d))
        result = set()
        for k in self.be:
            m, d = self.be.get_revision(k)
            m = tuple(sorted(m.items()))
            result.add((k, m, d.read()))
        assert result == expected_result

