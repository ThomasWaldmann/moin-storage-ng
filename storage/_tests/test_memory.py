# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - memory storage tests
"""

import pytest
from storage.memory import BytesStorage, FileStorage

@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_create( Storage):
    store = Storage()
    assert store._st is None

    store.create()
    assert store._st == {}

    return store

@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_destroy(Storage):
    store = test_create(Storage)
    store.destroy()
    assert store._st is None


