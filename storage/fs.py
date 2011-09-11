# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - filesystem storage
"""


from __future__ import absolute_import, division

import os, errno, shutil

from storage import MutableStorageBase


class Storage(MutableStorageBase):
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

    def mkpath(self, key):
        # XXX unsafe keys?
        return os.path.join(self.path, key)

    def __iter__(self):
        for key in os.listdir(self.path):
            yield key

    def get_bytes(self, key):
        f = self.get_file(key)
        try:
            return f.read() # better use get_file() and read smaller blocks for big files
        finally:
            f.close()

    def get_file(self, key):
        try:
            return open(self.mkpath(key), 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(key)
            raise

    def set_bytes(self, key, value):
        with open(self.mkpath(key), "wb") as f:
            f.write(value)

    def set_file(self, key, stream):
        try:
            with open(self.mkpath(key), "wb") as f:
                blocksize = 64 * 1024
                data = stream.read(blocksize)
                while data:
                    f.write(data)
                    data = stream.read(blocksize)
        finally:
            stream.close()

    def __delitem__(self, key):
        os.remove(self.mkpath(key))

