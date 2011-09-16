# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - router middleware tests
"""


from __future__ import absolute_import, division

from StringIO import StringIO

import pytest

from config import NAME, REVID

from middleware.router import Backend as RouterBackend
from backend.storages import MutableBackend as StorageBackend

from storage.memory import BytesStorage as MemoryBytesStorage
from storage.memory import FileStorage as MemoryFileStorage



def pytest_funcarg__router(request):
    root_be = StorageBackend(MemoryBytesStorage(), MemoryFileStorage())
    sub_be = StorageBackend(MemoryBytesStorage(), MemoryFileStorage())
    router = RouterBackend([('sub', sub_be), ('', root_be)])
    router.open()
    router.create()

    @request.addfinalizer
    def finalize():
        router.close()
        router.destroy()

    return router

def revid_split(revid):
    # router revids are <backend_mountpoint>/<backend_revid>, split that:
    return revid.rsplit(u'/', 1)

def test_store_get_del(router):
    root_name = u'foo'
    root_revid = router.store_revision(dict(name=root_name), StringIO(''))
    sub_name = u'sub/bar'
    sub_revid = router.store_revision(dict(name=sub_name), StringIO(''))

    assert revid_split(root_revid)[0] == ''
    assert revid_split(sub_revid)[0] == 'sub'

    # when going via the router backend, we get back fully qualified names:
    root_meta, _ = router.get_revision(root_revid)
    sub_meta, _ = router.get_revision(sub_revid)
    assert root_name == root_meta[NAME]
    assert sub_name == sub_meta[NAME]

    # when looking into the storage backend, we see relative names (without mountpoint):
    root_meta, _ = router.mapping[1][1].get_revision(revid_split(root_revid)[1])
    sub_meta, _ = router.mapping[0][1].get_revision(revid_split(sub_revid)[1])
    assert root_name == root_meta[NAME]
    assert sub_name == 'sub' + '/' + sub_meta[NAME]
    # delete revs:
    router.del_revision(root_revid)
    router.del_revision(sub_revid)



def test_iter(router):
    root_revid = router.store_revision(dict(name=u'foo'), StringIO(''))
    sub_revid = router.store_revision(dict(name=u'sub/bar'), StringIO(''))
    assert set(router) == set([root_revid, sub_revid])

