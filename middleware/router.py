# Copyright: 2008-2011 MoinMoin:ThomasWaldmann
# Copyright: 2009 MoinMoin:ChristopherDenter
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - router middleware

Routes requests to different backends depending on the item name.
"""


from __future__ import absolute_import, division

from config import NAME

from backend import BackendBase, MutableBackendBase

class Backend(BackendBase):
    """
    read-only router
    """
    def __init__(self, mapping):
        """
        Initialize router backend.

        The mapping given must satisfy the following criteria:
            * Order matters.
            * Mountpoints are just item names, including the special '' (empty)
              root item name.
            * Trailing '/' of a mountpoint will be stripped.
            * There *must* be a backend with mountpoint '' at the very
              end of the mapping. That backend is then used as root, which means
              that all items that don't lie in the namespace of any other
              backend are stored there.

        :type mapping: list of tuples of mountpoint -> backend mappings
        :param mapping: [(mountpoint, backend), ...]
        """
        self.mapping = [(mountpoint.rstrip('/'), backend) for mountpoint, backend in mapping]

    def open(self):
        pass

    def close(self):
        for mountpoint, backend in self.mapping:
            backend.close()

    def _get_backend(self, itemname):
        """
        For a given fully-qualified itemname (i.e. something like Company/Bosses/Mr_Joe)
        find the backend it belongs to (given by this instance's mapping), the local
        itemname inside that backend and the mountpoint of the backend.

        :param itemname: fully-qualified itemname
        :returns: tuple of (backend, local itemname, mountpoint)
        """
        for mountpoint, backend in self.mapping:
            if itemname == mountpoint or itemname.startswith(mountpoint and mountpoint + '/' or ''):
                lstrip = mountpoint and len(mountpoint)+1 or 0
                return backend, itemname[lstrip:], mountpoint
        raise AssertionError("No backend found for %r. Available backends: %r" % (itemname, self.mapping))

    def __iter__(self):
        # Note: yields <backend_mountpoint>/<backend_revid> as router revid, so that this
        #       can be given to get_revision and be routed to the right backend.
        for mountpoint, backend in self.mapping:
            for revid in backend:
                yield u'%s/%s' % (mountpoint, revid)

    def get_revision(self, revid):
        mountpoint, revid = revid.rsplit(u'/', 1)
        backend = self._get_backend(mountpoint)[0]
        meta, data = backend.get_revision(revid)
        if mountpoint:
            meta[NAME] = u'%s/%s' % (mountpoint, meta[NAME])
        return meta, data


class MutableBackend(Backend, MutableBackendBase):
    """
    read/write router
    """
    def create(self):
        for mountpoint, backend in self.mapping:
            backend.create()

    def destroy(self):
        for mountpoint, backend in self.mapping:
            backend.destroy()

    def store_revision(self, meta, data):
        itemname = meta[NAME]
        backend, itemname, mountpoint = self._get_backend(itemname)
        meta[NAME] = itemname
        return u'%s/%s' % (mountpoint, backend.store_revision(meta, data))

    def del_revision(self, revid):
        mountpoint, revid = revid.rsplit(u'/', 1)
        backend = self._get_backend(mountpoint)[0]
        backend.del_revision(revid)
