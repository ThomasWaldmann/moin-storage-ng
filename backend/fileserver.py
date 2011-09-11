# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend exposing part of the filesystem (read-only)
"""


from __future__ import absolute_import, division

import os, errno

from backend import BackendBase


class Backend(BackendBase):
    """
    exposes part of the filesystem (read-only)
    """
    def __init__(self, path):
        self.path = path

    def open(self):
        pass

    def close(self):
        pass

    def _mkpath(self, key):
        # XXX unsafe keys?
        return os.path.join(self.path, key)

    def __iter__(self):
        # TODO: flat right now, could be recursive
        for fn in os.listdir(self.path):
            yield fn

    def get_meta(self, fn):
        path = self._mkpath(fn)
        try:
            st = os.stat(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(fn)
            raise
        meta = {}
        meta['mtime'] = int(st.st_mtime) # use int, not float
        meta['size'] = int(st.st_size) # use int instead of long
        return meta

    def get_data(self, fn):
        path = self._mkpath(fn)
        try:
            return open(path, 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(fn)
            raise

    def get_revision(self, fn):
        meta = self.get_meta(fn)
        data = self.get_data(fn)
        return meta, data

