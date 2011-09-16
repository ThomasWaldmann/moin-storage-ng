# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - storage test magic
"""


from __future__ import absolute_import, division

import pytest
from storage.wrappers import ByteToStreamWrappingStore

# memcached is not in the loop
stores = 'fs kc memory sqlite sqlite:compressed'.split()


constructors = {
    'memory': lambda store, _: store(),
    'fs': lambda store, tmpdir: store(str(tmpdir.join('store'))),
    'sqlite': lambda store, tmpdir: store(str(tmpdir.join('store.sqlite')),
                                          'test_table', compression_level=0),
    'sqlite:compressed': lambda store, tmpdir: store(str(tmpdir.join('store.sqlite')),
                                          'test_table', compression_level=1),
    'kc': lambda store, tmpdir: store(str(tmpdir.join('store.kch'))),
}


def pytest_generate_tests(metafunc):
    argnames = metafunc.funcargnames
    
    if 'store' in argnames:
        klasses = 'BytesStorage', 'FileStorage'
    elif 'bst' in argnames:
        klasses = 'BytesStorage',
    elif 'fst' in argnames:
        klasses = 'FileStorage',
    else:
        klasses = None

    if klasses is not None:
        for storename in stores:
            for klass in klasses:
                metafunc.addcall(
                    id='%s/%s' % (storename, klass),
                    param=(storename, klass))

    multi_mark = getattr(metafunc.function, 'multi', None)
    if multi_mark is not None:
        # XXX: hack
        storages = multi_mark.kwargs['Storage']
        for storage in storages:
            metafunc.addcall(id=storage.__name__, funcargs={
                'Storage': storage,
            })


def make_storage(request):
    tmpdir = request.getfuncargvalue('tmpdir')
    storename, kind = request.param
    storemodule = pytest.importorskip('storage.' + storename.split(':')[0])
    klass = getattr(storemodule, kind)
    construct = constructors.get(storename)
    if construct is None:
        pytest.xfail('don\'t know how to construct %s store' % (storename,))
    store = construct(klass, tmpdir)
    store.create()
    store.open()
    # no destroy in the normal finalizer
    # so we can keep the data for example if it's a tmpdir
    request.addfinalizer(store.close)
    return store


def pytest_funcarg__bst(request):
    return make_storage(request)


def pytest_funcarg__fst(request):
    return make_storage(request)


def pytest_funcarg__store(request):
    store = make_storage(request)
    storename, kind = request.param
    if kind == 'FileStorage':
        store = ByteToStreamWrappingStore(store)
    return store

