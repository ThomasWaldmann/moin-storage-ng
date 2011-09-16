# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - sqlite storage tests
"""


import pytest
from storage.sqlite import BytesStorage, FileStorage

def bytes_compressed(path):
    return BytesStorage(path, 'test_table', compression_level=1)
def bytes_uncompressed(path):
    return BytesStorage(path, 'test_table', compression_level=0)

def file_compressed(path):
    return FileStorage(path, 'test_table', compression_level=1)
def file_uncompressed(path):
    return FileStorage(path, 'test_table', compression_level=0)

all_setups = pytest.mark.multi(Storage=[
    bytes_uncompressed,
    bytes_compressed,
    file_uncompressed,
    file_compressed,
])


@all_setups
def test_create(tmpdir, Storage):
    dbfile = tmpdir.join('store.sqlite')
    assert not dbfile.check()
    store = Storage(str(dbfile))
    assert not dbfile.check()
    store.create()
    assert dbfile.check()
    return store

@all_setups
def test_destroy(tmpdir, Storage):
    dbfile = tmpdir.join('store.sqlite')
    store = test_create(tmpdir, Storage)
    store.destroy()
    # XXX: check for dropped table

