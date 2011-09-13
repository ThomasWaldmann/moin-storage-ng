# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - indexing middleware tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO
import hashlib

import pytest

from config import NAME, SIZE, ITEMID, REVID, DATAID, HASH_ALGORITHM, CONTENT, COMMENT

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

    def test_create_revision(self):
        item_name = u'foo'
        data = 'bar'
        item = self.imw[item_name]
        rev = item.create_revision(dict(name=item_name), StringIO(data))
        revid = rev.revid
        # check if we have the revision now:
        item = self.imw[item_name]
        assert item # does exist
        rev = item.get_revision(revid)
        assert rev.meta[NAME] == item_name
        assert rev.data.read() == data
        revids = list(item.iter_revs())
        assert revids == [revid]

    def test_clear_revision(self):
        item_name = u'foo'
        data = 'bar'
        item = self.imw[item_name]
        rev = item.create_revision(dict(name=item_name), StringIO(data))
        revid = rev.revid
        # clear revision:
        reason = u'just cleared'
        item.clear_revision(revid, reason=reason)
        # check if the revision was cleared:
        item = self.imw[item_name]
        rev = item.get_revision(revid)
        assert rev.meta[NAME] == item_name
        assert rev.meta[COMMENT] == reason
        assert rev.data.read() == ''
        revids = list(item.iter_revs())
        assert len(revids) == 1 # we still have the revision, cleared
        assert revid in revids # it is still same revid

    def test_destroy_revision(self):
        item_name = u'foo'
        item = self.imw[item_name]
        rev = item.create_revision(dict(name=item_name), StringIO('bar'))
        revid_destroyed = rev.revid
        rev = item.create_revision(dict(name=item_name), StringIO('baz'))
        revid_left = rev.revid
        # destroy revision:
        item.destroy_revision(revid_destroyed)
        # check if the revision was destroyed:
        item = self.imw[item_name]
        with pytest.raises(KeyError):
            item.get_revision(revid_destroyed)
        revs = list(item.iter_revs())
        assert revs == [revid_left]

    def test_destroy_item(self):
        revids = []
        item_name = u'foo'
        item = self.imw[item_name]
        rev = item.create_revision(dict(name=item_name), StringIO('bar'))
        revids.append(rev.revid)
        rev = item.create_revision(dict(name=item_name), StringIO('baz'))
        revids.append(rev.revid)
        # destroy item:
        item.destroy_item()
        # check if the item was destroyed:
        item = self.imw[item_name]
        assert not item # does not exist

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

    def test_index_rebuild(self):
        # first we index some stuff the slow "on-the-fly" way:
        expected_latest_revids = []
        item_name = u'foo'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=1), StringIO('does not count, different name'))
        expected_latest_revids.append(r.revid)
        item_name = u'bar'
        item = self.imw[item_name]
        item.create_revision(dict(name=item_name, mtime=1), StringIO('1st'))
        r = item.create_revision(dict(name=item_name, mtime=2), StringIO('2nd'))
        expected_latest_revids.append(r.revid)

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
        all_docs = list(self.imw.documents(all_revs=True))
        latest_docs = list(self.imw.documents(all_revs=False))
        latest_revids = [doc[REVID] for doc in latest_docs]

        print "*** all rebuilt:"
        self.imw.dump(all_revs=True)
        print "*** latest rebuilt:"
        self.imw.dump(all_revs=False)

        # should be all the same, order does not matter:
        assert sorted(expected_all_docs) == sorted(all_docs)
        assert sorted(expected_latest_docs) == sorted(latest_docs)
        assert sorted(latest_revids) == sorted(expected_latest_revids)

    def test_index_update(self):
        # first we index some stuff the slow "on-the-fly" way:
        expected_all_revids = []
        expected_latest_revids = []
        missing_revids = []
        item_name = u'updated'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=1), StringIO('updated 1st'))
        expected_all_revids.append(r.revid)
        # we update this item below, so we don't add it to expected_latest_revids
        #NOTE: we don't have destroy_revision yet (that makes the rev vanish completely)
        #item_name = u'destroyed'
        #item = self.imw[item_name]
        #r = item.create_revision(dict(name=item_name, mtime=1), StringIO('destroyed 1st'))
        ## we destroy this item below, so we don't add it to expected_all_revids
        ## we update this item below, so we don't add it to expected_latest_revids
        item_name = u'stayssame'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=1), StringIO('stayssame 1st'))
        expected_all_revids.append(r.revid)
        # we update this item below, so we don't add it to expected_latest_revids
        r = item.create_revision(dict(name=item_name, mtime=2), StringIO('stayssame 2nd'))
        expected_all_revids.append(r.revid)
        expected_latest_revids.append(r.revid)

        # now build a fresh index at tmp location:
        self.imw.create(tmp=True)
        self.imw.rebuild(tmp=True)

        # while the fresh index still sits at the tmp location, we update and add some items.
        # this will not change the fresh index, but the old index we are still using.
        item_name = u'updated'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=2), StringIO('updated 2nd'))
        expected_all_revids.append(r.revid)
        expected_latest_revids.append(r.revid)
        missing_revids.append(r.revid)
        item_name = u'added'
        item = self.imw[item_name]
        r = item.create_revision(dict(name=item_name, mtime=1), StringIO('added 1st'))
        expected_all_revids.append(r.revid)
        expected_latest_revids.append(r.revid)
        missing_revids.append(r.revid)
        #NOTE: we don't have a destroy_revision yet
        #item_name = u'destroyed'
        #item = self.imw[item_name]
        #item.destroy_revision(destroy_revid)

        # now switch to the not-quite-fresh-any-more index we have built:
        self.imw.close()
        self.imw.move_index()
        self.imw.open()

        # read the index contents we have now:
        all_revids = [doc[REVID] for doc in self.imw.documents(all_revs=True)]
        latest_revids = [doc[REVID] for doc in self.imw.documents(all_revs=False)]

        # this index is outdated:
        for missing_revid in missing_revids:
            assert missing_revid not in all_revids
            assert missing_revid not in latest_revids

        # update the index:
        self.imw.close()
        self.imw.update()
        self.imw.open()

        # read the index contents we have now:
        all_revids = [doc[REVID] for doc in self.imw.documents(all_revs=True)]
        latest_revids = [doc[REVID] for doc in self.imw.documents(all_revs=False)]

        # now it should have the previously missing rev and all should be as expected:
        for missing_revid in missing_revids:
            assert missing_revid in all_revids
            assert missing_revid in latest_revids
        assert sorted(all_revids) == sorted(expected_all_revids)
        assert sorted(latest_revids) == sorted(expected_latest_revids)

    def test_revision_contextmanager(self):
        # check if rev.data is closed after leaving the with-block
        item_name = u'foo'
        meta = dict(name=item_name)
        data = 'some test content'
        item = self.imw[item_name]
        data_file = StringIO(data)
        with item.create_revision(meta, data_file) as rev:
            assert rev.data.read() == data
            revid = rev.revid
        with pytest.raises(ValueError):
            rev.data.read()
        with item.get_revision(revid) as rev:
            assert rev.data.read() == data
        with pytest.raises(ValueError):
            rev.data.read()

    def test_indexed_content(self):
        # TODO: this is a very simple check that assumes that data is put 1:1
        # into index' CONTENT field.
        item_name = u'foo'
        meta = dict(name=item_name)
        data = 'some test content'
        item = self.imw[item_name]
        data_file = StringIO(data)
        with item.create_revision(meta, data_file) as rev:
            expected_revid = rev.revid
        doc = self.imw.document(content=u'test')
        assert expected_revid == doc[REVID]
        assert unicode(data) == doc[CONTENT]

