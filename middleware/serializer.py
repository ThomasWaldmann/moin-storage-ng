import struct
import json
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


def deserialize(io, backend):
    while True:
        meta_size_bytes = io.read(4)
        if not meta_size_bytes:
            return
        meta_size = struct.unpack('!i', meta_size_bytes)[0]
        meta_str = io.read(meta_size)
        text = meta_str.decode('utf-8')
        meta = json.loads(text)
        data_size = meta[u'size']
        
        #XXX: this shoul be, unreliable for unknown reason
        #limited = LimitedStream(io, data_size)
        #backend.store_revision(meta, limited)
        #assert limited.is_exhausted

        data = io.read(data_size)
        from StringIO import StringIO
        backend.store_revision(meta, StringIO(data))
        

