# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - filesystem storage
"""


from __future__ import absolute_import, division

import os, errno, shutil
from StringIO import StringIO

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
        try:
            with open(self.mkpath(key), 'rb') as f:
                return f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(key)
            raise

    def get_file(self, key):
        return StringIO(self.get_bytes(key))

    def set_bytes(self, key, value):
        with open(self.mkpath(key), "wb") as f:
            f.write(value)

    def set_file(self, key, stream):
        value = stream.read()
        stream.close()
        self.set_bytes(key, value)

    def __delitem__(self, key):
        os.remove(self.mkpath(key))

