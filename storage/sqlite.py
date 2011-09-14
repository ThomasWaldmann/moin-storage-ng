# Copyright: 2011 MoinMoin:ThomasWaldmann
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - sqlite3 storage
"""


from __future__ import absolute_import, division

from StringIO import StringIO
from sqlite3 import *

from storage import MutableStorageBase, BytesMutableStorageBase, FileMutableStorageBase

class _Storage(MutableStorageBase):
    """
    A simple sqlite3 based storage.
    """
    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name

    def create(self):
        conn = connect(self.db_name)
        with conn:
            conn.execute('create table %s (key text primary key, value blob)' % self.table_name)

    def destroy(self):
        conn = connect(self.db_name)
        with conn:
            conn.execute('drop table %s' % self.table_name)

    def open(self):
        self.conn = connect(self.db_name)
        self.conn.row_factory = Row # make column access by ['colname'] possible

    def close(self):
        pass

    def __iter__(self):
        for row in self.conn.execute("select key from %s" % self.table_name):
            yield row['key']

    def __delitem__(self, key):
        with self.conn:
            self.conn.execute('delete from %s where key=?' % self.table_name, (key, ))


class BytesStorage(_Storage, BytesMutableStorageBase):
    def __getitem__(self, key):
        rows = list(self.conn.execute("select value from %s where key=?" % self.table_name, (key, )))
        if not rows:
            raise KeyError(key)
        return rows[0]['value']

    def __setitem__(self, key, value):
        with self.conn:
            self.conn.execute('insert into %s values (?, ?)' % self.table_name, (key, value))


class FileStorage(_Storage, FileMutableStorageBase):
    def __getitem__(self, key):
        rows = list(self.conn.execute("select value from %s where key=?" % self.table_name, (key, )))
        if not rows:
            raise KeyError(key)
        return StringIO(rows[0]['value'])

    def __setitem__(self, key, stream):
        value = stream.read()
        with self.conn:
            self.conn.execute('insert into %s values (?, ?)' % self.table_name, (key, value))

