# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# Copyright: 2011 MoinMoin:MichaelMayorov
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - indexing middleware

The backends and storages moin uses are rather simple, it is mostly just a
unsorted / unordered bunch of meta and data with iteration.

The indexer middleware adds the needed power: after all metadata and data
is indexed, we can do all sorts of operations on the indexer level:
* searching
* lookup by name, uuid, ...
* selecting
* listing
"""

# TODO: no "routing" yet, all is expected to be in one backend.


from __future__ import absolute_import, division

import os
import shutil
import itertools
import time, datetime

from uuid import uuid4

make_uuid = lambda: unicode(uuid4().hex)
UUID_LEN = len(make_uuid())

import logging

from whoosh.fields import Schema, TEXT, ID, IDLIST, NUMERIC, DATETIME, KEYWORD, BOOLEAN
from whoosh.index import open_dir, create_in, EmptyIndexError
from whoosh.writing import AsyncWriter
from whoosh.filedb.multiproc import MultiSegmentWriter
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.query import Every

from config import WIKINAME, NAME, NAME_EXACT, MTIME, CONTENTTYPE, TAGS, \
                   LANGUAGE, USERID, ADDRESS, HOSTNAME, SIZE, ACTION, COMMENT, \
                   CONTENT, ITEMLINKS, ITEMTRANSCLUSIONS, ACL, EMAIL, OPENID, \
                   ITEMID, REVID

LATEST_REVS = 'latest_revs'
ALL_REVS = 'all_revs'
INDEXES = [LATEST_REVS, ALL_REVS, ]


def backend_to_index(meta, content, schema, wikiname):
    """
    Convert backend metadata/data to a whoosh document.

    :param meta: revision meta from moin storage
    :param content: revision data converted to indexable content
    :param schema: whoosh schema
    :param wikiname: interwikiname of this wiki
    :returns: document to put into whoosh index
    """

    doc = dict([(str(key), value)
                for key, value in meta.items()
                if key in schema])
    # TODO not there yet: doc[MTIME] = datetime.datetime.utcfromtimestamp(doc[MTIME])
    doc[NAME_EXACT] = doc[NAME]
    doc[WIKINAME] = wikiname
    doc[CONTENT] = content
    return doc


def convert_to_indexable(meta, data):
    """
    Convert revision data to a indexable content.

    :param meta: revision metadata (gets updated as a side effect)
    :param data: revision data (file-like)
                 please make sure that the content file is
                 ready to read all indexable content from it. if you have just
                 written that content or already read from it, you need to call
                 rev.seek(0) before calling convert_to_indexable(rev).
    :returns: indexable content, text/plain, unicode object
    """
    # stringio sucks
    return u'' # TODO integrate real thing after merge into moin2 code base.


class IndexingMiddleware(object):
    def __init__(self, index_dir, backend, **kw):
        """
        Store params, create schemas.
        """
        self.index_dir = index_dir
        self.index_dir_tmp = index_dir + '.temp'
        self.backend = backend
        self.wikiname = u'' # TODO take from app.cfg.interwikiname
        self.ix = {}  # open indexes
        self.schemas = {}  # existing schemas

        common_fields = {
            # wikiname so we can have a shared index in a wiki farm, always check this!
            WIKINAME: ID(stored=True),
            # tokenized NAME from metadata - use this for manual searching from UI
            # TODO was: NAME: TEXT(stored=True, multitoken_query="and", analyzer=item_name_analyzer(), field_boost=2.0),
            NAME: ID(stored=True, field_boost=2.0),
            # unmodified NAME from metadata - use this for precise lookup by the code.
            # also needed for wildcard search, so the original string as well as the query
            # (with the wildcard) is not cut into pieces.
            NAME_EXACT: ID(field_boost=3.0),
            # revision id (aka meta id)
            REVID: ID(unique=True, stored=True),
            # MTIME from revision metadata (converted to UTC datetime)
            MTIME: DATETIME(stored=True),
            # tokenized CONTENTTYPE from metadata
            # TODO was: CONTENTTYPE: TEXT(stored=True, multitoken_query="and", analyzer=MimeTokenizer()),
            CONTENTTYPE: ID(stored=True),
            # unmodified list of TAGS from metadata
            TAGS: ID(stored=True),
            # LANGUAGE from metadata
            LANGUAGE: ID(stored=True),
            # USERID from metadata
            USERID: ID(stored=True),
            # ADDRESS from metadata
            ADDRESS: ID(stored=True),
            # HOSTNAME from metadata
            HOSTNAME: ID(stored=True),
            # SIZE from metadata
            SIZE: NUMERIC(stored=True),
            # ACTION from metadata
            ACTION: ID(stored=True),
            # tokenized COMMENT from metadata
            COMMENT: TEXT(stored=True),
            # data (content), converted to text/plain and tokenized
            CONTENT: TEXT(stored=True),
        }

        latest_revs_fields = {
            # ITEMID from metadata - as there is only latest rev of same item here, it is unique
            ITEMID: ID(unique=True, stored=True),
            # unmodified list of ITEMLINKS from metadata
            ITEMLINKS: ID(stored=True),
            # unmodified list of ITEMTRANSCLUSIONS from metadata
            ITEMTRANSCLUSIONS: ID(stored=True),
            # tokenized ACL from metadata
            # TODO was: ACL: TEXT(analyzer=AclTokenizer(self._cfg), multitoken_query="and", stored=True),
            ACL: ID(stored=True),
        }
        latest_revs_fields.update(**common_fields)

        userprofile_fields = {
            # EMAIL from user profile metadata
            EMAIL: ID(unique=True, stored=True),
            # OPENID from user profile metadata
            OPENID: ID(unique=True, stored=True),
        }
        latest_revs_fields.update(**userprofile_fields)

        all_revs_fields = {
            # UUID from metadata
            ITEMID: ID(stored=True),
        }
        all_revs_fields.update(**common_fields)

        latest_revisions_schema = Schema(**latest_revs_fields)
        all_revisions_schema = Schema(**all_revs_fields)

        # Define dynamic fields
        dynamic_fields = [("*_id", ID(stored=True)),
                          ("*_text", TEXT(stored=True)),
                          ("*_keyword", KEYWORD(stored=True)),
                          ("*_numeric", NUMERIC(stored=True)),
                          ("*_datetime", DATETIME(stored=True)),
                          ("*_boolean", BOOLEAN(stored=True)),
                         ]

        # Adding dynamic fields to schemas
        for glob, field_type in dynamic_fields:
            latest_revisions_schema.add(glob, field_type, glob=True)
            all_revisions_schema.add(glob, field_type, glob=True)

        # schemas are needed by query parser and for index creation
        self.schemas[ALL_REVS] = all_revisions_schema
        self.schemas[LATEST_REVS] = latest_revisions_schema

    def open(self):
        """
        Open all indexes.
        """
        index_dir = self.index_dir
        try:
            for name in INDEXES:
                self.ix[name] = open_dir(index_dir, indexname=name)
        except (IOError, OSError, EmptyIndexError) as err:
            logging.error(u"%s [while trying to open index '%s' in '%s']" % (str(err), name, index_dir))
            raise

    def close(self):
        """
        Close all indexes.
        """
        for name in self.ix:
            self.ix[name].close()
        self.ix = {}

    def create(self, tmp=False):
        """
        Create all indexes (empty).
        """
        index_dir = self.index_dir_tmp if tmp else self.index_dir
        try:
            os.mkdir(index_dir)
        except:
            # ignore exception, we'll get another exception below
            # in case there are problems with the index_dir
            pass
        try:
            for name in INDEXES:
                create_in(index_dir, self.schemas[name], indexname=name)
        except (IOError, OSError) as err:
            logging.error(u"%s [while trying to create index '%s' in '%s']" % (str(err), name, index_dir))
            raise

    def destroy(self, tmp=False):
        """
        Destroy all indexes.
        """
        index_dir = self.index_dir_tmp if tmp else self.index_dir
        if os.path.exists(index_dir):
            shutil.rmtree(index_dir)

    def move_index(self):
        """
        Move freshly built indexes from index_dir_tmp to index_dir.
        """
        self.destroy()
        os.rename(self.index_dir_tmp, self.index_dir)

    def index_revision(self, revid, meta, data):
        """
        Index a single revision, add it to all-revs and latest-revs index.
        """
        meta[REVID] = revid
        content = convert_to_indexable(meta, data)
        with AsyncWriter(self.ix[ALL_REVS]) as writer:
            doc = backend_to_index(meta, content, self.schemas[ALL_REVS], self.wikiname)
            writer.add_document(**doc)
        with AsyncWriter(self.ix[LATEST_REVS]) as writer:
            doc = backend_to_index(meta, content, self.schemas[LATEST_REVS], self.wikiname)
            writer.update_document(**doc)

    def rebuild(self, tmp=False, procs=1, limitmb=256):
        """
        Add all items/revisions from the backends of this wiki to the index
        (which is expected to have no items/revisions from this wiki yet).
        
        Note: index might be shared by multiple wikis, so it is:
              create, rebuild wiki1, rebuild wiki2, ...
              create (tmp), rebuild wiki1, rebuild wiki2, ..., move
        """
        def build_index(index_dir, indexname, schema, wikiname, revids, procs=1, limitmb=256):
            ix = open_dir(index_dir, indexname=indexname)
            if procs == 1:
                # MultiSegmentWriter sometimes has issues and is pointless for procs == 1,
                # so use the simple writer when --procs 1 is given:
                writer = ix.writer()
            else:
                writer = MultiSegmentWriter(ix, procs, limitmb)
            with writer as writer:
                for revid in revids:
                    meta, data = self.backend.get_revision(revid)
                    content = convert_to_indexable(meta, data)
                    doc = backend_to_index(meta, content, schema, wikiname)
                    writer.add_document(**doc)

        index_dir = self.index_dir_tmp if tmp else self.index_dir
        # first we build an index of all we have (so we know what we have)
        all_revids = self.backend # the backend is a iterator over all revids
        build_index(index_dir, ALL_REVS, self.schemas[ALL_REVS], self.wikiname, all_revids, procs, limitmb)

        
        index = open_dir(self.index_dir, indexname=ALL_REVS)
        latest_revids = []  # TODO: idea: search for Everything in all-revs, sortedby itemid, mtime
        with index.searcher() as searcher:
            result = searcher.search(Every(), groupedby=ITEMID, sortedby=[MTIME], reverse=True)
            by_item = result.groups(ITEMID)
            for val in by_item.values():
                latest_revids.append(searcher.stored_fields(val[0])[REVID])
        build_index(index_dir, LATEST_REVS, self.schemas[LATEST_REVS], self.wikiname, latest_revids, procs, limitmb)

    def update(self):
        """
        Make sure index reflects current backend state, add missing stuff, remove outdated stuff.
        """
        # TODO

    def get_schema(self, all_revs=False):
        # XXX keep this as is for now, but later just give the index name as param
        name = ALL_REVS if all_revs else LATEST_REVS
        return self.schemas[name]

    def get_index(self, all_revs=False):
        # XXX keep this as is for now, but later just give the index name as param
        name = ALL_REVS if all_revs else LATEST_REVS
        return self.ix[name]

    def dump(self, all_revs=False):
        """
        Output all documents in index to stdout (most useful for debugging).
        """
        ix = self.get_index(all_revs)
        with ix.searcher() as searcher:
            for doc in searcher.all_stored_fields():
                name = doc.pop(NAME, u"")
                content = doc.pop(CONTENT, u"")
                for field, value in [(NAME, name), ] + sorted(doc.items()) + [(CONTENT, content), ]:
                    print "%s: %s" % (field, repr(value)[:70])
                print

    def query_parser(self, default_fields, all_revs=False):
        """
        Build a query parser for a list of default fields.
        """
        schema = self.get_schema(all_revs)
        if len(default_fields) > 1:
            qp = MultifieldParser(default_fields, schema=schema)
        elif len(default_fields) == 1:
            qp = QueryParser(default_fields[0], schema=schema)
        else:
            raise ValueError("default_fields list must at least contain one field name")
        return qp

    def search(self, q, all_revs=False, **kw):
        """
        Search with query q, yield stored fields.
        """
        with self.get_index(all_revs).searcher() as searcher:
            # Note: callers must consume everything we yield, so the for loop
            # ends and the "with" is left to close the index files.
            for hit in searcher.search(q, **kw):
                yield hit.fields()

    def search_page(self, q, all_revs=False, pagenum=1, pagelen=10, **kw):
        """
        Same as search, but with paging support.
        """
        with self.get_index(all_revs).searcher() as searcher:
            # Note: callers must consume everything we yield, so the for loop
            # ends and the "with" is left to close the index files.
            for hit in searcher.search_page(q, pagenum, pagelen=pagelen, **kw):
                yield hit.fields()

    def documents(self, all_revs=False, **kw):
        """
        Yield documents matching the kw args.
        """
        with self.get_index(all_revs).searcher() as searcher:
            # Note: callers must consume everything we yield, so the for loop
            # ends and the "with" is left to close the index files.
            if kw:
                for doc in searcher.documents(**kw):
                    yield doc
            else: # XXX maybe this would make sense to be whoosh default behaviour for documents()?
                for doc in searcher.all_stored_fields():
                    yield doc


    def document(self, all_revs=False, **kw):
        """
        Return document matching the kw args.
        """
        with self.get_index(all_revs).searcher() as searcher:
            return searcher.document(**kw)

    def __getitem__(self, item_name):
        """
        Return item with <item_name> (may be a new or existing item).
        """
        return Item(self, item_name)

    def create_item(self, item_name):
        """
        Return item with <item_name> (must be a new item).
        """
        return Item.create(self, item_name)

    def existing_item(self, item_name):
        """
        Return item with <item_name> (must be an existing item).
        """
        return Item.existing(self, item_name)


class Item(object):
    def __init__(self, router, item_name):
        self.router = router
        self.item_name = item_name
        self.backend = self.router.backend # TODO was: backend_for_path(itemname)
        doc = self.router.document(all_revs=False, name=item_name)
        if doc:
            self.itemid = doc[ITEMID]
            self.current_revision = doc[REVID]
        else:
            self.itemid = None
            self.current_revision = None

    @classmethod
    def create(cls, router, item_name):
        """
        Create a new item and return it, raise exception if it already exists.
        """
        item = cls(router, item_name)
        if not item:
            return item
        raise ItemAlreadyExists(item_name)
        
    @classmethod
    def existing(cls, router, item_name):
        """
        Get an existing item and return it, raise exception if it does not exist.
        """
        item = cls(router, item_name)
        if item:
            return item
        raise ItemDoesNotExist(item_name)

    def __nonzero__(self):
        """
        Item exists (== has at least one revision)?
        """
        return self.itemid is not None

    def iter_revs(self):
        """
        Iterate over revids belonging to this item (use index).
        """
        if self:
            for doc in self.router.documents(all_revs=True, itemid=self.itemid):
                yield doc[REVID]

    def __getitem__(self, revid):
        """
        Get Revision with revision id <revid>.
        """
        return Revision(self, revid)

    def get_revision(self, revid):
        """
        Same as item[revid].
        """
        return self[revid]

    def create_revision(self, meta, data):
        """
        Create a new revision, write metadata and data to it.

        :type meta: dict
        :type data: str or stream (stream will be closed after use)
        """
        if self.itemid is None:
            self.itemid = make_uuid()
        meta[ITEMID] = self.itemid
        # later: backend_name, backend = self.router.backend_rest(self.name)
        backend = self.backend
        revid = backend.store_revision(meta, data)
        self.router.index_revision(revid, meta, data)
        self.current_revision = revid
        return Revision(self, revid, meta, data)

    def destroy_revision(self, revid, reason=None):
        """
        Check if revision with that revid exists (via index) -
        if yes, destroy revision with that revid.
        if no, raise RevisionDoesNotExistError.

        Note: we destroy the data and most of the metadata values, but keep "reason" in some rudimentary metadata
        """
        # TODO (data reference?)
        
    def destroy(self, reason=None):
        """
        Destroy all revisions of this item.
        """
        for revid in self.iter_revs():
            self.destroy_revision(revid, reason)


class Revision(object):
    """
    An existing revision (exists in the backend).
    """
    def __init__(self, item, revid, meta=None, data=None):
        self.item = item
        self.revid = revid
        self.backend = item.backend
        if meta is None and data is None:
            self.meta, self.data = self.backend.get_revision(self.revid) # raises KeyError if rev does not exist
        else:
            # Item.create_revision() gives us meta and data, so we don't need to access the backend again
            self.meta, self.data = meta, data


