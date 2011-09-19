# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - protecting middleware tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO

import pytest

from config import ACL

from ..protecting import ProtectingMiddleware, AccessDenied

from .test_indexing import TestIndexingMiddleware


class TestProtectingMiddleware(TestIndexingMiddleware):
    def setup_method(self, method):
        super(TestProtectingMiddleware, self).setup_method(method)
        self.imw = ProtectingMiddleware(self.imw, user_name=u'joe')

    def teardown_method(self, method):
        self.imw = self.imw.indexer
        super(TestProtectingMiddleware, self).teardown_method(method)

    def _dummy(self):
        # replacement for tests that use unsupported methods / attributes
        pass

    test_index_rebuild = _dummy
    test_index_update = _dummy
    test_indexed_content = _dummy

    def test_documents(self):
        item_name = u'public'
        item = self.imw[item_name]
        r = item.store_revision(dict(name=item_name, acl=u'joe:read'), StringIO('public content'))
        revid_public = r.revid
        item_name = u'secret'
        item = self.imw[item_name]
        r = item.store_revision(dict(name=item_name, acl=u''), StringIO('secret content'))
        revid_secret = r.revid
        revids = [rev.revid for rev in self.imw.documents(all_revs=False)]
        assert revids == [revid_public]  # without revid_secret!

    def test_getitem(self):
        item_name = u'public'
        item = self.imw[item_name]
        r = item.store_revision(dict(name=item_name, acl=u'joe:read'), StringIO('public content'))
        revid_public = r.revid
        item_name = u'secret'
        item = self.imw[item_name]
        r = item.store_revision(dict(name=item_name, acl=u'boss:read'), StringIO('secret content'))
        revid_secret = r.revid
        # now testing:
        item_name = u'public'
        item = self.imw[item_name]
        r = item[revid_public]
        assert r.data.read() == 'public content'
        item_name = u'secret'
        item = self.imw[item_name]
        with pytest.raises(AccessDenied):
            r = item[revid_secret]

