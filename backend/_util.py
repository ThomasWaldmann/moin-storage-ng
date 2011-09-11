# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend utilities
"""


from __future__ import absolute_import, division

import hashlib


class TrackingFileWrapper(object):
    """
    Wraps a file and computes hashcode and file size while it is read.
    Requires that initially the realfile is open and at pos 0.
    Users need to call .read(blocksize) until it does not return any more data.
    After this self.hash and self.size will have the wanted values.
    self.hash is the hash instance, you may want to call self.hash.hexdigest().
    Finally, you must call .close().
    """
    def __init__(self, realfile, hash_method='sha1'):
        self._realfile = realfile
        self.hash = hashlib.new(hash_method)
        self.size = 0

    def read(self, size=-1):
        data = self._realfile.read(size)
        self.hash.update(data)
        self.size += len(data)
        return data

    def close(self):
        self._realfile.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

