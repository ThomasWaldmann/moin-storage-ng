# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - backend exposing part of the filesystem (read-only)
"""


from __future__ import absolute_import, division

import os, errno, stat
from StringIO import StringIO

from backend import BackendBase


class Backend(BackendBase):
    """
    exposes part of the filesystem (read-only)
    """
    def __init__(self, path):
        self.path = unicode(path)

    def open(self):
        pass

    def close(self):
        pass

    def _mkpath(self, key):
        # XXX unsafe keys?
        return os.path.join(self.path, key)

    def _mkkey(self, path):
        root = self.path
        assert path.startswith(root)
        key = path[len(root)+1:]
        return key

    def __iter__(self):
        for dirpath, dirnames, filenames in os.walk(self.path):
            key = self._mkkey(dirpath)
            if key:
                yield key
            for filename in filenames:
                yield self._mkkey(os.path.join(dirpath, filename))

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
        if stat.S_ISDIR(st.st_mode):
            # directory
            # we create a virtual wiki page listing links to subitems:
            ct = 'text/x.moin.wiki;charset=utf-8'
            size = 0
        elif stat.S_ISREG(st.st_mode):
            # normal file
            # TODO: real mimetype guessing
            if fn.endswith('.png'):
                ct = 'image/png'
            elif fn.endswith('.txt'):
                ct = 'text/plain'
            else:
                ct = 'application/octet-stream'
            size = int(st.st_size) # use int instead of long
        else:
            # symlink, device file, etc.
            ct = 'application/octet-stream'
            size = 0
        meta['contenttype'] = ct
        meta['size'] = size
        return meta

    def _make_directory_page(self, path):
        try:
            dirs = []
            files = []
            names = os.listdir(path)
            for name in names:
                filepath = os.path.join(path, name)
                if os.path.isdir(filepath):
                    dirs.append(name)
                else:
                    files.append(name)
            content = [
                u"= Directory contents =",
                u" * [[../]]",
            ]
            content.extend(u" * [[/%s|%s/]]" % (name, name) for name in sorted(dirs))
            content.extend(u" * [[/%s|%s]]" % (name, name) for name in sorted(files))
            content.append(u"")
            content = u'\r\n'.join(content)
        except OSError as err:
            content = unicode(err)
        return content

    def get_data(self, fn):
        path = self._mkpath(fn)
        try:
            st = os.stat(path)
            if stat.S_ISDIR(st.st_mode):
                data = self._make_directory_page(path)
                return StringIO(data.encode('utf-8'))
            elif stat.S_ISREG(st.st_mode):
                return open(path, 'rb')
            else:
                return StringIO('')
        except (OSError, IOError) as e:
            if e.errno == errno.ENOENT:
                raise KeyError(fn)
            raise

    def get_revision(self, fn):
        meta = self.get_meta(fn)
        data = self.get_data(fn)
        return meta, data

