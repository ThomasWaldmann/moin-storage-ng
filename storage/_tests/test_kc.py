# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - kyoto cabinet storage tests
"""


from __future__ import absolute_import, division

import pytest
pytest.importorskip('storage.kc')

from storage.kc import BytesStorage, FileStorage


@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_create(tmpdir, Storage):
    target = tmpdir.join('store.kc')
    assert not target.check()

    store = Storage(str(target))
    assert not target.check()
    store.create()
    assert target.check()

    return store


@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_destroy(tmpdir, Storage):
    store = test_create(tmpdir, Storage)
    target = tmpdir.join('store.kc')
    store.destroy()
    assert not target.check()


