# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend serialization / deserialization

We use a simple custom format here:

4 bytes length of meta (m)
m bytes metadata (json serialization, utf-8 encoded)
        (the metadata contains the data length d in meta['size'])
d bytes binary data
... (repeat for all meta/data)
4 bytes 00 (== length of next meta -> there is none, this is the end)
"""


from __future__ import absolute_import, division

import struct
import json
from io import BytesIO

from werkzeug.wsgi import LimitedStream


def serialize(backend, dst):
    dst.writelines(serialize_iter(backend))


def serialize_iter(backend):
    for revid in backend:
        meta, data = backend.retrieve(revid)

        text = json.dumps(meta, ensure_ascii=False)
        meta_str = text.encode('utf-8')
        yield struct.pack('!i', len(meta_str))
        yield meta_str
        while True:
            block = data.read(8192)
            if not block:
                break
            yield block
    yield struct.pack('!i', 0)


def deserialize(src, backend):
    while True:
        meta_size_bytes = src.read(4)
        meta_size = struct.unpack('!i', meta_size_bytes)[0]
        if not meta_size:
            return
        meta_str = src.read(meta_size)
        text = meta_str.decode('utf-8')
        meta = json.loads(text)
        data_size = meta[u'size']

        limited = LimitedStream(src, data_size)
        backend.store(meta, limited)
        assert limited.is_exhausted

