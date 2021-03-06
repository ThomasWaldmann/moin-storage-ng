# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - sqla store tests
"""


import pytest

from ..sqla import BytesStore, FileStore

@pytest.mark.multi(Store=[BytesStore, FileStore])
def test_create(tmpdir, Store):
    dbfile = tmpdir.join('store.sqlite')
    assert not dbfile.check()
    store = Store('sqlite:///%s' % str(dbfile))
    assert not dbfile.check()
    store.create()
    assert dbfile.check()
    return store

@pytest.mark.multi(Store=[BytesStore, FileStore])
def test_destroy(tmpdir, Store):
    dbfile = tmpdir.join('store.sqlite')
    store = test_create(tmpdir, Store)
    store.destroy()
    # XXX: check for dropped table

