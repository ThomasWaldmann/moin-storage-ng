# Copyright: 2011 MoinMoin:RonnyPfannschmidt
# Copyright: 2011 MoinMoin:ThomasWaldmann
# Copyright: 2011 MoinMoin:MichaelMayorov
# License: GNU GPL v2 (or any later version), see LICENSE.txt for details.

"""
MoinMoin - indexing middleware

The backends and storages moin uses are rather simple, it is mostly just a
unsorted / unordered bunch of revisions (meta and data) with iteration.

The indexer middleware adds the needed power: after all metadata and data
is indexed, we can do all sorts of operations on the indexer level:
* searching
* lookup by name, uuid, ...
* selecting
* listing
"""


from __future__ import absolute_import, division

import os
import shutil
import itertools
import time, datetime
from StringIO import StringIO

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
from whoosh.sorting import FieldFacet

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
    if MTIME in doc:
        # we have UNIX UTC timestamp (int), whoosh wants datetime
        doc[MTIME] = datetime.datetime.utcfromtimestamp(doc[MTIME])
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
    return unicode(data.read()) # TODO integrate real thing after merge into moin2 code base.


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
            LANGUAGE: ID(stored=True),
            # USERID from metadata TODO: -> user ITEMID
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
            EMAIL: ID(unique=True, stored=True),
            OPENID: ID(unique=True, stored=True),
        }
        latest_revs_fields.update(**userprofile_fields)

        all_revs_fields = {
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

    def index_revision(self, revid, meta, data, async=True):
        """
        Index a single revision, add it to all-revs and latest-revs index.
        """
        meta[REVID] = revid
        content = convert_to_indexable(meta, data)
        if async:
            writer = AsyncWriter(self.ix[ALL_REVS])
        else:
            writer = self.ix[ALL_REVS].writer()
        with writer as writer:
            doc = backend_to_index(meta, content, self.schemas[ALL_REVS], self.wikiname)
            writer.update_document(**doc) # clear_revision() gives us an existing revid
        if async:
            writer = AsyncWriter(self.ix[LATEST_REVS])
        else:
            writer = self.ix[LATEST_REVS].writer()
        with writer as writer:
            doc = backend_to_index(meta, content, self.schemas[LATEST_REVS], self.wikiname)
            writer.update_document(**doc)

    def del_revision(self, revid, async=True):
        """
        Delete a single revision from indexes.
        """
        if async:
            writer = AsyncWriter(self.ix[ALL_REVS])
        else:
            writer = self.ix[ALL_REVS].writer()
        with writer as writer:
            writer.delete_by_term(REVID, revid)
        if async:
            writer = AsyncWriter(self.ix[LATEST_REVS])
        else:
            writer = self.ix[LATEST_REVS].writer()
        with writer as writer:
            # TODO: if the revid was in the latest revs index, we remove it by
            # this. we should add whatever is the latest rev now.
            writer.delete_by_term(REVID, revid)

    def _modify_index(self, index, schema, wikiname, revids, mode='add', procs=1, limitmb=256):
        """
        modify index contents - add, update, delete the indexed documents for all given revids

        Note: mode == 'add' is faster but you need to make sure to not create duplicate
              documents in the index.
        """
        if procs == 1:
            # MultiSegmentWriter sometimes has issues and is pointless for procs == 1,
            # so use the simple writer when --procs 1 is given:
            writer = index.writer()
        else:
            writer = MultiSegmentWriter(index, procs, limitmb)
        with writer as writer:
            for revid in revids:
                if mode in ['add', 'update', ]:
                    meta, data = self.backend.get_revision(revid)
                    content = convert_to_indexable(meta, data)
                    doc = backend_to_index(meta, content, schema, wikiname)
                if mode == 'update':
                    writer.update_document(**doc)
                elif mode == 'add':
                    writer.add_document(**doc)
                elif mode == 'delete':
                    writer.delete_by_term(REVID, revid)

    def rebuild(self, tmp=False, procs=1, limitmb=256):
        """
        Add all items/revisions from the backends of this wiki to the index
        (which is expected to have no items/revisions from this wiki yet).
        
        Note: index might be shared by multiple wikis, so it is:
              create, rebuild wiki1, rebuild wiki2, ...
              create (tmp), rebuild wiki1, rebuild wiki2, ..., move
        """
        index_dir = self.index_dir_tmp if tmp else self.index_dir
        index = open_dir(index_dir, indexname=ALL_REVS)
        try:
            # build an index of all we have (so we know what we have)
            all_revids = self.backend # the backend is a iterator over all revids
            self._modify_index(index, self.schemas[ALL_REVS], self.wikiname, all_revids, 'add', procs, limitmb)

            # now, using the freshly built index, determine the latest revisions for all items:
            latest_revids = []
            with index.searcher() as searcher:
                result = searcher.search(Every(), groupedby=ITEMID, sortedby=FieldFacet(MTIME, reverse=True))
                by_item = result.groups(ITEMID)
                for _, vals in by_item.items():
                    # XXX figure how whoosh can order, or get the best
                    vals.sort(key=lambda docid:searcher.stored_fields(docid)[MTIME], reverse=True)
                    latest_revids.append(searcher.stored_fields(vals[0])[REVID])
        finally:
            index.close()
        # now build the index of the latest revisions:
        index = open_dir(index_dir, indexname=LATEST_REVS)
        try:
            self._modify_index(index, self.schemas[LATEST_REVS], self.wikiname, latest_revids, 'add', procs, limitmb)
        finally:
            index.close()

    def update(self, tmp=False):
        """
        Make sure index reflects current backend state, add missing stuff, remove outdated stuff.

        This is intended to be used:
        * after a full rebuild that was done at tmp location
        * after wiki is made read-only or taken offline
        * after the index was moved to the normal index location
        
        Reason: new revisions that were created after the rebuild started might be missing in new index.
        """
        index_dir = self.index_dir_tmp if tmp else self.index_dir
        index_all = open_dir(index_dir, indexname=ALL_REVS)
        try:
            # first update ALL_REVS index:
            backend_revids = set(self.backend)
            with index_all.searcher() as searcher:
                ix_revids = set([doc[REVID] for doc in searcher.all_stored_fields()])
            todo_revids = backend_revids - ix_revids
            self._modify_index(index_all, self.schemas[ALL_REVS], self.wikiname, todo_revids, 'add')

            index_latest = open_dir(index_dir, indexname=LATEST_REVS)
            try:
                # now update LATEST_REVS index:
                # determine itemid -> latest_revid mappings:
                with index_all.searcher() as searcher:
                    tmp = {}
                    for doc in searcher.all_stored_fields():
                        tmp.setdefault(doc[ITEMID], []).append((doc[MTIME], doc[REVID]))
                    backend_revids = {}
                    for itemid, mtimes_revids in tmp.items():
                        revid = sorted(mtimes_revids, reverse=True)[0][1]
                        backend_revids[itemid] = revid
                with index_latest.searcher() as searcher:
                    ix_revids = dict((doc[ITEMID], doc[REVID]) for doc in searcher.all_stored_fields())
                #print "backend itemid -> revid", sorted(backend_revids.items())
                #print "index   itemid -> revid", sorted(ix_revids.items())
                backend_itemids = set(backend_revids)
                ix_itemids = set(ix_revids)
                add_itemids = backend_itemids - ix_itemids
                del_itemids = ix_itemids - backend_itemids
                upd_itemids = set([itemid for itemid in ix_itemids & backend_itemids
                                   if backend_revids[itemid] != ix_revids[itemid]])
                #print "itemids to add   :", add_itemids
                #print "itemids to delete:", del_itemids
                #print "itemids to update:", upd_itemids
                for mode, itemids in [('update', upd_itemids), ('delete', del_itemids), ('add', add_itemids)]:
                    revids = [backend_revids[itemid] for itemid in itemids]
                    self._modify_index(index_latest, self.schemas[LATEST_REVS], self.wikiname, revids, mode)
            finally:
                index_latest.close()
        finally:
            index_all.close()

    def optimize_storage(self):
        """
        Optimize storage / collect garbage to safe space:

        * trash bin: empty it? use trash_max_age?
        * user profiles: only keep latest revision?
        * normal wiki items: keep by max_revisions_count / max_age
        * deduplicate data (determine dataids with same hash, fix references to point to one of them)
        * remove unreferenced dataids (destroyed revisions, deduplicated stuff)
        """
        # TODO

    def optimize_index(self):
        """
        Optimize whoosh index.
        """
        # XXX unclear: do we need to close the index before optimizing it? or lock it so it is not used otherwise?
        for name in self.ix:
            self.ix[name].optimize()

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
    def __init__(self, indexer, item_name):
        self.indexer = indexer
        self.item_name = item_name
        self.backend = self.indexer.backend
        doc = self.indexer.document(all_revs=False, name=item_name)
        if doc:
            self.itemid = doc[ITEMID]
            self.current_revision = doc[REVID]
        else:
            self.itemid = None
            self.current_revision = None

    @classmethod
    def create(cls, indexer, item_name):
        """
        Create a new item and return it, raise exception if it already exists.
        """
        item = cls(indexer, item_name)
        if not item:
            return item
        raise ItemAlreadyExists(item_name)
        
    @classmethod
    def existing(cls, indexer, item_name):
        """
        Get an existing item and return it, raise exception if it does not exist.
        """
        item = cls(indexer, item_name)
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
            for doc in self.indexer.documents(all_revs=True, itemid=self.itemid):
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
        :type data: open file (file must be closed by caller)
        """
        if self.itemid is None:
            self.itemid = make_uuid()
        meta[ITEMID] = self.itemid
        backend = self.backend
        revid = backend.store_revision(meta, data)
        data.seek(0)  # rewind file
        self.indexer.index_revision(revid, meta, data)
        self.current_revision = revid
        return Revision(self, revid)

    def clear_revision(self, revid, reason=None):
        """
        Check if revision with that revid exists (via index) -
        if yes, clear revision with that revid.
        if no, raise RevisionDoesNotExistError.

        Note: "clear" means: we modify the revision in the backend, so most metadata
              values are clear, the reason is put into the comment and the data is empty.
        """
        backend = self.backend
        meta, data = backend.get_revision(revid) # raises KeyError if rev does not exist
        meta[COMMENT] = reason or u'destroyed'
        # TODO cleanup more metadata
        data = StringIO('') # nothing to see there
        revid = backend.store_revision(meta, data)
        # Note: we just stored new (empty) data for this revision, but the old
        # data file is still in storage (not referenced by THIS revision any more)
        data.seek(0)  # rewind file
        self.indexer.index_revision(revid, meta, data)
        
    def clear_item(self, reason=None):
        """
        Clear all revisions of this item.
        """
        for revid in self.iter_revs():
            self.clear_revision(revid, reason)

    def destroy_revision(self, revid):
        """
        Check if revision with that revid exists (via index) -
        if yes, destroy revision with that revid.
        if no, raise RevisionDoesNotExistError.

        Note: "destroy" means: we delete the revision from the backend
        """
        self.backend.del_revision(revid)
        self.indexer.del_revision(revid)
        
    def destroy_item(self):
        """
        Destroy all revisions of this item.
        """
        for revid in self.iter_revs():
            self.destroy_revision(revid)


class Revision(object):
    """
    An existing revision (exists in the backend).
    """
    def __init__(self, item, revid):
        self.item = item
        self.revid = revid
        self.backend = item.backend
        self.meta, self.data = self.backend.get_revision(self.revid) # raises KeyError if rev does not exist

    def close(self):
        self.data.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

