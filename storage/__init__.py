# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - storage base classes
"""


from __future__ import absolute_import, division

import io
from abc import abstractmethod
from collections import Mapping, MutableMapping


class StorageBase(Mapping):
    """
    A read-only storage backend is a simple key/value store.
    """
    def __init__(self, **kw):
        """
        lazy stuff - just remember pathes, urls, database name, ... -
        whatever we need for open(), create(), etc.
        """

    def open(self):
        """
        open the storage, prepare it for usage
        """

    def close(self):
        """
        close the storage, stop using it, free resources (except stored data)
        """

    @abstractmethod
    def __iter__(self):
        """
        iterate over keys present in the storage
        """

    def __len__(self):
        return len([key for key in self])

    @abstractmethod
    def __getitem__(self, key):
        """
        return data stored for key
        """


class BytesStorageBase(StorageBase):
    @abstractmethod
    def __getitem__(self, key):
        """
        return bytestring for key if exists else raise KeyError
        """


class FileStorageBase(StorageBase):
    @abstractmethod
    def __getitem__(self, key):
        """
        return a filelike for key if exists else raise KeyError
        """


class MutableStorageBase(StorageBase, MutableMapping):
    """
    A read/write storage backend is a simple key/value store.
    """
    def create(self):
        """
        create an empty storage
        """

    def destroy(self):
        """
        destroy the storage (erase all stored data, remove storage)
        """

    @abstractmethod
    def __setitem__(self, key, value):
        """
        store value under key
        """

    @abstractmethod
    def __delitem__(self, key):
        """
        delete the key, dereference the related value in the storage
        """


class BytesMutableStorageBase(MutableStorageBase):
    @abstractmethod
    def __setitem__(self, key):
        """
        store a bytestring for key
        """


class FileMutableStorageBase(MutableStorageBase):
    @abstractmethod
    def __setitem__(self, key):
        """
        store a filelike for key
        """

