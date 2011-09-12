# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - indexing middleware tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO
import hashlib

import pytest

from config import NAME, SIZE, ITEMID, REVID, DATAID, HASH_ALGORITHM

from middleware.indexing import IndexingMiddleware
from backend.storages import MutableBackend

from storage.memory import BytesStorage as MemoryBytesStorage
from storage.memory import FileStorage as MemoryFileStorage

class TestIndexingMiddleware(object):
    def setup_method(self, method):
        meta_store = MemoryBytesStorage()
        data_store = MemoryFileStorage()
        self.be = MutableBackend(meta_store, data_store)
        self.be.create()
        self.be.open()
        index_dir = 'ix'
        self.imw = IndexingMiddleware(index_dir, self.be)
        self.imw.create()
        self.imw.open()

    def teardown_method(self, method):
        self.imw.close()
        self.imw.destroy()
        self.be.close()
        self.be.destroy()

    def test_nonexisting_item(self):
        item = self.imw[u'foo']
        assert not item # does not exist

    def test_existing_item(self):
        item_name = u'foo'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('bar'))
        item = self.imw[item_name]
        assert item # does exist

    def test_all_revisions(self):
        item_name = u'foo'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('does not count, different name'))
        item_name = u'bar'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('1st'))
        item.create_revision(dict(name=item_name), StringIO('2nd'))
        item = self.imw[item_name]
        revs = [item[revid].data.read() for revid in item.iter_revs()]
        assert len(revs) == 2
        assert set(revs) == set(['1st', '2nd'])

    def test_latest_revision(self):
        item_name = u'foo'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('does not count, different name'))
        item_name = u'bar'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('1st'))
        expected_rev = item.create_revision(dict(name=item_name), StringIO('2nd'))
        docs = list(self.imw.documents(all_revs=False, name=item_name))
        assert len(docs) == 1  # there is only 1 latest revision
        assert expected_rev.revid == docs[0][REVID]  # it is really the latest one

    def test_auto_meta(self):
        item_name = u'foo'
        data = 'bar'
        item = self.imw[item_name]
        rev = item.create_revision(dict(name=item_name), StringIO(data))
        rev = item[rev.revid]
        print repr(rev.meta)
        assert rev.meta[NAME] == item_name
        assert rev.meta[SIZE] == len(data)
        assert rev.meta[HASH_ALGORITHM] == hashlib.new(HASH_ALGORITHM, data).hexdigest()
        assert ITEMID in rev.meta
        assert REVID in rev.meta
        assert DATAID in rev.meta

    def test_documents(self):
        item_name = u'foo'
        item = self.imw[item_name]
        rev1 = item.create_revision(dict(name=item_name), StringIO('x'))
        rev2 = item.create_revision(dict(name=item_name), StringIO('xx'))
        rev3 = item.create_revision(dict(name=item_name), StringIO('xxx'))
        doc = self.imw.document(all_revs=True, size=2)
        assert doc
        assert doc[REVID] == rev2.revid
        docs = list(self.imw.documents(all_revs=True, size=2))
        assert len(docs) == 1
        assert docs[0][REVID] == rev2.revid

    def test_rebuild(self):
        expected_latest_revids = []
        
        # first we index some stuff the slow "on-the-fly" way:
        item_name = u'foo'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=1), StringIO('does not count, different name'))

        expected_latest_revids.append(r.revid)

        item_name = u'bar'
        item = self.imw[item_name]
        #XXX: mtime/parents
        item.create_revision(dict(name=item_name, mtime=1), StringIO('1st'))
        r = item.create_revision(dict(name=item_name, mtime=2), StringIO('2nd'))

        expected_latest_revids.append(r.revid)
        expected_latest_revids.sort()
        # now we remember the index contents built that way:
        expected_latest_docs = list(self.imw.documents(all_revs=False))
        expected_all_docs = list(self.imw.documents(all_revs=True))
        print "*** all on-the-fly:"
        self.imw.dump(all_revs=True)
        print "*** latest on-the-fly:"
        self.imw.dump(all_revs=False)
        # now kill the index and do a full rebuild
        self.imw.close()
        self.imw.destroy()
        self.imw.create()
        self.imw.rebuild()
        self.imw.open()
        # read the index contents built that way:
        latest_docs = list(self.imw.documents(all_revs=False))
        all_docs = list(self.imw.documents(all_revs=True))
        print "*** all rebuilt:"
        self.imw.dump(all_revs=True)
        print "*** latest rebuilt:"
        self.imw.dump(all_revs=False)
        # should be both the same, order does not matter:
        assert sorted(expected_all_docs) == sorted(all_docs)
        assert sorted(expected_latest_docs) == sorted(latest_docs)

        latest_revids = sorted(x[REVID] for x in latest_docs)
        assert latest_revids == expected_latest_revids

