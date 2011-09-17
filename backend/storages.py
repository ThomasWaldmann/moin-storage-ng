# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend using 2 storages

Usually, this will be a ByteStorage for meta and a FileStorage for data.

But, you could also use other combinations, you just need to be prepared
for the revision data datatype it returns (that is exactly what the data_store
returns) and also feed it with the correct revision data datatype (which
is what the data_store accepts).
"""


from __future__ import absolute_import, division

from uuid import uuid4

make_uuid = lambda: unicode(uuid4().hex)
UUID_LEN = len(make_uuid())

from config import REVID, HASH_ALGORITHM

from backend import BackendBase, MutableBackendBase
from backend._util import TrackingFileWrapper

try:
    import json
except ImportError:
    import simplejson as json


class Backend(BackendBase):
    """
    ties together a store for metadata and a store for data, readonly
    """
    def __init__(self, meta_store, data_store):
        self.meta_store = meta_store
        self.data_store = data_store

    def open(self):
        self.meta_store.open()
        self.data_store.open()

    def close(self):
        self.meta_store.close()
        self.data_store.close()

    def __iter__(self):
        for metaid in self.meta_store:
            yield metaid

    def _deserialize(self, meta_str):
        text = meta_str.decode('utf-8')
        meta = json.loads(text)
        return meta

    def _get_meta(self, metaid):
        meta = self.meta_store[metaid]
        # XXX Idea: we could check the type we get from the store:
        # if it is a str/bytes, just use it "as is",
        # if it is a file, read and close it (so we have a str/bytes).
        return self._deserialize(meta)

    def _get_data(self, dataid):
        data = self.data_store[dataid]
        # XXX Idea: we could check the type we get from the store:
        # if it is a file, just return it "as is",
        # if it is a str/bytes, wrap it into StringIO (so we always return
        # a file-like object).
        return data

    def retrieve(self, metaid):
        meta = self._get_meta(metaid)
        dataid = meta['dataid']
        data = self._get_data(dataid)
        return meta, data


class MutableBackend(Backend, MutableBackendBase):
    """
    same as Backend, but read/write
    """
    def create(self):
        self.meta_store.create()
        self.data_store.create()

    def destroy(self):
        self.meta_store.destroy()
        self.data_store.destroy()

    def _serialize(self, meta):
        text = json.dumps(meta, ensure_ascii=False)
        meta_str = text.encode('utf-8')
        return meta_str

    def _store_meta(self, meta):
        if REVID not in meta:
            # Item.clear_revision calls us with REVID already present
            meta[REVID] = make_uuid()
        metaid = meta[REVID]
        meta = self._serialize(meta)
        # XXX Idea: we could check the type the store wants from us:
        # if it is a str/bytes (BytesStorage), just use meta "as is",
        # if it is a file (FileStorage), wrap it into StringIO and give that to the store.
        self.meta_store[metaid] = meta
        return metaid

    def store(self, meta, data):
        # XXX Idea: we could check the type the store wants from us:
        # if it is a str/bytes (BytesStorage), just use meta "as is",
        # if it is a file (FileStorage), wrap it into StringIO and give that to the store.
        if 'dataid' not in meta:
            tfw = TrackingFileWrapper(data, hash_method=HASH_ALGORITHM)
            dataid = make_uuid()
            self.data_store[dataid] = tfw
            meta['dataid'] = dataid
            meta['size'] = tfw.size
            meta[HASH_ALGORITHM] = tfw.hash.hexdigest()
        else:
            dataid = meta['dataid']
            # we will just asume stuff is correct if you pass it with a data id
            if dataid not in self.data_store:
                self.data_store[dataid] = data
        # if something goes wrong below, the data shall be purged by a garbage collection
        metaid = self._store_meta(meta)
        return metaid

    def _del_meta(self, metaid):
        del self.meta_store[metaid]

    def _del_data(self, dataid):
        del self.data_store[dataid]

    def remove(self, metaid):
        meta = self._get_meta(metaid)
        dataid = meta['dataid']
        self._del_meta(metaid)
        self._del_data(dataid)

