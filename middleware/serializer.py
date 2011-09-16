import struct
import json
from io import BytesIO

from werkzeug.wsgi import LimitedStream


def serialize(backend, targetfile):
    targetfile.writelines(serialize_iter(backend))


def serialize_iter(backend):
    for revid in backend:
        meta, data = backend.get_revision(revid)

        text = json.dumps(meta, ensure_ascii=False)
        meta_str = text.encode('utf-8')
        yield struct.pack('!i', len(meta_str))
        yield meta_str
        while True:
            block = data.read(8192)
            if not block:
                break
            yield block
    # the deserializer expects next meta len here, but gets 0 as
    # indication of a valid end of stream:
    yield struct.pack('!i', 0)


def deserialize(io, backend):
    while True:
        meta_size_bytes = io.read(4)
        meta_size = struct.unpack('!i', meta_size_bytes)[0]
        if not meta_size:
            return
        meta_str = io.read(meta_size)
        text = meta_str.decode('utf-8')
        meta = json.loads(text)
        data_size = meta[u'size']

        limited = LimitedStream(io, data_size)
        backend.store_revision(meta, limited)
        assert limited.is_exhausted


