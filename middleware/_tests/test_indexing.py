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

    def test_revisions(self):
        item_name = u'foo'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('does not count, different name'))
        item_name = u'bar'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name), StringIO('1st'))
        item.create_revision(dict(name=item_name), StringIO('2nd'))
        item = self.imw[item_name]
        revs = [item.get_revision(revid)[1].read() for revid in item.iter_revs()]
        assert len(revs) == 2
        assert set(revs) == set(['1st', '2nd'])

    def test_auto_meta(self):
        item_name = u'foo'
        data = 'bar'
        item = self.imw[item_name]
        revid = item.create_revision(dict(name=item_name), StringIO(data))
        meta, _ = item.get_revision(revid)
        print repr(meta)
        assert meta[NAME] == item_name
        assert meta[SIZE] == len(data)
        assert meta[HASH_ALGORITHM] == hashlib.new(HASH_ALGORITHM, data).hexdigest()
        assert ITEMID in meta
        assert REVID in meta
        assert DATAID in meta

    def test_documents(self):
        item_name = u'foo'
        item = self.imw[item_name]
        revid1 = item.create_revision(dict(name=item_name), StringIO('x'))
        revid2 = item.create_revision(dict(name=item_name), StringIO('xx'))
        revid3 = item.create_revision(dict(name=item_name), StringIO('xxx'))
        doc = self.imw.document(all_revs=True, size=2)
        assert doc
        assert doc[REVID] == revid2
        docs = list(self.imw.documents(all_revs=True, size=2))
        assert len(docs) == 1
        assert docs[0][REVID] == revid2

