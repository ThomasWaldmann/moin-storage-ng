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
    def get_file(self, key):
        """
        return a filelike for key if exists else raise KeyError
        """
    
    @abstractmethod
    def get_bytes(self, key):
        """
        return bytestring for key if exists else raise KeyError
        """

    def __getitem__(self, key):
        return self.get_bytes(key)


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
    def set_file(self, key, stream):
        """
        store a filelike for key
        """
    
    @abstractmethod
    def set_bytes(self, key):
        """
        store a bytestring for key
        """

    def __setitem__(self, key, value):
        """
        store value under key
        """
        if isinstance(value, io.IOBase):
            self.set_file(key, value)
        elif isinstance(value, bytes):
            self.set_bytes(key, value)
        else:
            raise TypeError("%r is not bytes or filelike" % (value.__class__, ))

    @abstractmethod
    def __delitem__(self, key):
        """
        delete the key, dereference the related value in the storage
        """

