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
        curs = conn.cursor()
        curs.execute('create table %s (key text primary key, value blob)' % self.table_name)
        conn.commit()
        curs.close()

    def destroy(self):
        conn = connect(self.db_name)
        curs = conn.cursor()
        curs.execute('drop table %s' % self.table_name)
        conn.commit()
        curs.close()

    def open(self):
        self.conn = connect(self.db_name)
        self.curs = self.conn.cursor()

    def close(self):
        self.curs.close()

    def __iter__(self):
        self.curs.execute("select key from %s" % self.table_name)
        for row in self.curs.fetchall():
            key = row[0]
            yield key

    def __delitem__(self, key):
        self.curs.execute('delete from %s where key=?' % self.table_name, (key, ))
        self.conn.commit()


class BytesStorage(_Storage, BytesMutableStorageBase):
    def __getitem__(self, key):
        self.curs.execute("select value from %s where key=?" % self.table_name, (key, ))
        row = self.curs.fetchone()
        if row is None:
            raise KeyError(key)
        value = row[0]
        return value

    def __setitem__(self, key, value):
        self.curs.execute('insert into %s values (?, ?)' % self.table_name, (key, value))
        self.conn.commit()


class FileStorage(_Storage, FileMutableStorageBase):
    def __getitem__(self, key):
        self.curs.execute("select value from %s where key=?" % self.table_name, (key, ))
        row = self.curs.fetchone()
        if row is None:
            raise KeyError(key)
        value = row[0]
        return StringIO(value)

    def __setitem__(self, key, stream):
        value = stream.read()
        self.curs.execute('insert into %s values (?, ?)' % self.table_name, (key, value))
        self.conn.commit()

