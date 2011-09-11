# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - filesystem storage
"""


from __future__ import absolute_import, division

import os, errno, shutil

from storage import MutableStorageBase, BytesMutableStorageBase, FileMutableStorageBase


class _Storage(MutableStorageBase):
    """
    A simple filesystem-based storage.

    keys are required to be valid filenames.
    """
    def __init__(self, path):
        self.path = path

    def create(self):
        os.mkdir(self.path)

    def destroy(self):
        shutil.rmtree(self.path)

    def _mkpath(self, key):
        # XXX unsafe keys?
        return os.path.join(self.path, key)

    def __iter__(self):
        for key in os.listdir(self.path):
            yield key

    def __delitem__(self, key):
        os.remove(self._mkpath(key))


class BytesStorage(_Storage, BytesMutableStorageBase):
    def __getitem__(self, key):
        try:
            with open(self._mkpath(key), 'rb') as f:
                return f.read() # better use get_file() and read smaller blocks for big files
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(key)
            raise

    def __setitem__(self, key, value):
        with open(self._mkpath(key), "wb") as f:
            f.write(value)


class FileStorage(_Storage, FileMutableStorageBase):
    def __getitem__(self, key):
        try:
            return open(self._mkpath(key), 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(key)
            raise

    def __setitem__(self, key, stream):
        try:
            with open(self._mkpath(key), "wb") as f:
                blocksize = 64 * 1024
                data = stream.read(blocksize)
                while data:
                    f.write(data)
                    data = stream.read(blocksize)
        finally:
            stream.close()

