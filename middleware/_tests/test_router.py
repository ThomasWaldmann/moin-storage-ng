# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - router middleware tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO

import pytest

from config import NAME, REVID

from middleware.router import MutableBackend as RouterBackend
from backend.storages import MutableBackend as StorageBackend

from storage.memory import BytesStorage as MemoryBytesStorage
from storage.memory import FileStorage as MemoryFileStorage


class TestIndexingMiddleware(object):
    def setup_method(self, method):
        self.root_be = StorageBackend(MemoryBytesStorage(), MemoryFileStorage())
        self.sub_be = StorageBackend(MemoryBytesStorage(), MemoryFileStorage())
        self.be = RouterBackend([('sub', self.sub_be), ('', self.root_be)])
        self.be.create()
        self.be.open()

    def teardown_method(self, method):
        self.be.close()
        self.be.destroy()

    def test_store_get_del(self):
        root_name = u'foo'
        root_revid = self.be.store_revision(dict(name=root_name), StringIO(''))
        sub_name = u'sub/bar'
        sub_revid = self.be.store_revision(dict(name=sub_name), StringIO(''))

        def revid_split(revid):
            # router revids are <backend_mountpoint>/<backend_revid>, split that:
            return revid.rsplit(u'/', 1)

        assert revid_split(root_revid)[0] == ''
        assert revid_split(sub_revid)[0] == 'sub'
        # when going via the router backend, we get back fully qualified names:
        root_meta, _ = self.be.get_revision(root_revid)
        sub_meta, _ = self.be.get_revision(sub_revid)
        assert root_name == root_meta[NAME]
        assert sub_name == sub_meta[NAME]
        # when looking into the storage backend, we see relative names (without mountpoint):
        root_meta, _ = self.root_be.get_revision(revid_split(root_revid)[1])
        sub_meta, _ = self.sub_be.get_revision(revid_split(sub_revid)[1])
        assert root_name == root_meta[NAME]
        assert sub_name == 'sub' + '/' + sub_meta[NAME]
        # delete revs:
        self.be.del_revision(root_revid)
        self.be.del_revision(sub_revid)

    def test_iter(self):
        root_revid = self.be.store_revision(dict(name=u'foo'), StringIO(''))
        sub_revid = self.be.store_revision(dict(name=u'sub/bar'), StringIO(''))
        assert set(list(self.be)) == set([root_revid, sub_revid])

