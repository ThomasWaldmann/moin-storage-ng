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

UNPROTECTED = u'unprotected'
PROTECTED = u'protected'

UNPROTECTED_CONTENT = 'unprotected content'
PROTECTED_CONTENT = 'protected content'

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

    def make_items(self, unprotected_acl, protected_acl):
        items = [(UNPROTECTED, unprotected_acl, UNPROTECTED_CONTENT),
                 (PROTECTED, protected_acl, PROTECTED_CONTENT),
                ]
        revids = []
        for item_name, acl, content in items:
            item = self.imw[item_name]
            r = item.store_revision(dict(name=item_name, acl=acl), StringIO(content))
            revids.append(r.revid)
        return revids

    def test_documents(self):
        revid_unprotected, revid_protected = self.make_items(u'joe:read', u'boss:read')
        revids = [rev.revid for rev in self.imw.documents(all_revs=False)]
        assert revids == [revid_unprotected]  # without revid_protected!
    
    def test_getitem(self):
        revid_unprotected, revid_protected = self.make_items(u'joe:read', u'boss:read')
        # now testing:
        item = self.imw[UNPROTECTED]
        r = item[revid_unprotected]
        assert r.data.read() == UNPROTECTED_CONTENT
        item = self.imw[PROTECTED]
        with pytest.raises(AccessDenied):
            r = item[revid_protected]

    def test_write(self):
        revid_unprotected, revid_protected = self.make_items(u'joe:write', u'boss:write')
        # now testing:
        item = self.imw[UNPROTECTED]
        item.store_revision(dict(name=UNPROTECTED, acl=u'joe:write'), StringIO(UNPROTECTED_CONTENT))
        item = self.imw[PROTECTED]
        with pytest.raises(AccessDenied):
            item.store_revision(dict(name=PROTECTED, acl=u'boss:write'), StringIO(UNPROTECTED_CONTENT))

    def test_write_create(self):
        # now testing:
        item_name = u'newitem'
        item = self.imw[item_name]
        item.store_revision(dict(name=item_name), StringIO('new content'))

    def test_overwrite(self):
        revid_unprotected, revid_protected = self.make_items(u'joe:write joe:overwrite', u'boss:write boss:overwrite')
        # now testing:
        item = self.imw[UNPROTECTED]
        item.store_revision(dict(name=UNPROTECTED, acl=u'joe:write joe:overwrite', revid=revid_unprotected),
                            StringIO(UNPROTECTED_CONTENT), overwrite=True)
        item = self.imw[PROTECTED]
        with pytest.raises(AccessDenied):
            item.store_revision(dict(name=PROTECTED, acl=u'boss:write boss:overwrite', revid=revid_protected),
                                StringIO(UNPROTECTED_CONTENT), overwrite=True)

    def test_destroy(self):
        revid_unprotected, revid_protected = self.make_items(u'joe:destroy', u'boss:destroy')
        # now testing:
        item = self.imw[UNPROTECTED]
        item.destroy_all_revisions()
        item = self.imw[PROTECTED]
        with pytest.raises(AccessDenied):
            item.destroy_all_revisions()


