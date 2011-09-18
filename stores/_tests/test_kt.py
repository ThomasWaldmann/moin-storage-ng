# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - kyoto tycoon storage tests
"""


from __future__ import absolute_import, division

import pytest
pytest.importorskip('storage.kt')

from storage.kt import BytesStorage, FileStorage


@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_create(Storage):
    store = Storage()
    store.create()
    return store


@pytest.mark.multi(Storage=[BytesStorage, FileStorage])
def test_destroy(Storage):
    store = Storage()
    store.destroy()

