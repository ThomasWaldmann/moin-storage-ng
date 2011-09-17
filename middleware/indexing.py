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

We also check ACLs here. Index has ALL content, so we must be careful not
to show data from index to a user that is not allowed to read that data.
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
from whoosh.query import Every, Term
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
    def __init__(self, index_dir, backend, user_name=None, acl_support=False, **kw):
        """
        Store params, create schemas.
        """
        self.index_dir = index_dir
        self.index_dir_tmp = index_dir + '.temp'
        self.backend = backend
        self.user_name = user_name # TODO use currently logged-in username
        self.acl_support = acl_support
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
        doc = backend_to_index(meta, content, self.schemas[ALL_REVS], self.wikiname)
        if async:
            writer = AsyncWriter(self.ix[ALL_REVS])
        else:
            writer = self.ix[ALL_REVS].writer()
        with writer as writer:
            writer.update_document(**doc) # clear_revision() gives us an existing revid
        doc = backend_to_index(meta, content, self.schemas[LATEST_REVS], self.wikiname)
        if async:
            writer = AsyncWriter(self.ix[LATEST_REVS])
        else:
            writer = self.ix[LATEST_REVS].writer()
        with writer as writer:
            writer.update_document(**doc)

    def remove_revision(self, revid, async=True):
        """
        Remove a single revision from indexes.
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
            # find out itemid related to the revid we want to remove:
            with self.ix[LATEST_REVS].searcher() as searcher:
                docnum_remove = searcher.document_number(revid=revid)
                if docnum_remove is not None:
                    itemid = searcher.stored_fields(docnum_remove)[ITEMID]
            if docnum_remove is not None:
                # we are removing a revid that is in latest revs index
                try:
                    latest_revids = self._find_latest_revids(self.ix[ALL_REVS], Term(ITEMID, itemid))
                except AttributeError:
                    # workaround for bug #200 AttributeError: 'FieldCache' object has no attribute 'code'
                    latest_revids = []
                if latest_revids:
                    # we have a latest revision, just update the document in the index:
                    assert len(latest_revids) == 1 # this item must have only one latest revision
                    latest_revid = latest_revids[0]
                    # we must fetch from backend because schema for LATEST_REVS is different than for ALL_REVS
                    # (and we can't be sure we have all fields stored, too)
                    meta, _ = self.backend.retrieve(latest_revid)
                    # we only use meta (not data), because we do not want to transform data->content again (this
                    # is potentially expensive) as we already have the transformed content stored in ALL_REVS index:
                    with self.ix[ALL_REVS].searcher() as searcher:
                        doc = searcher.document(revid=latest_revid)
                        content = doc[CONTENT]
                    doc = backend_to_index(meta, content, self.schemas[LATEST_REVS], self.wikiname)
                    writer.update_document(**doc)
                else:
                    # this is no revision left in this item that could be the new "latest rev", just kill the rev
                    writer.delete_document(docnum_remove)

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
                    meta, data = self.backend.retrieve(revid)
                    content = convert_to_indexable(meta, data)
                    doc = backend_to_index(meta, content, schema, wikiname)
                if mode == 'update':
                    writer.update_document(**doc)
                elif mode == 'add':
                    writer.add_document(**doc)
                elif mode == 'delete':
                    writer.delete_by_term(REVID, revid)
                else:
                    raise ValueError("mode must be 'update', 'add' or 'delete', not '%s'" % mode)

    def _find_latest_revids(self, index, query=None):
        """
        find the latest revids from the backend

        :param index: an up-to-date and open ALL_REVS index
        :param query: query to search only specific revisions (optional, default: all revisions)
        :returns: a list of the latest revids
        """
        if query is None:
            query = Every()
        # now, using the freshly built index, determine the latest revisions for all items:
        with index.searcher() as searcher:
            result = searcher.search(query, groupedby=ITEMID, sortedby=FieldFacet(MTIME, reverse=True))
            by_item = result.groups(ITEMID)
            latest_revids = []
            for _, vals in by_item.items():
                # XXX figure how whoosh can order, or get the best
                vals.sort(key=lambda docid: searcher.stored_fields(docid)[MTIME], reverse=True)
                latest_revid = searcher.stored_fields(vals[0])[REVID]
                latest_revids.append(latest_revid)
        return latest_revids

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
            latest_revids = self._find_latest_revids(index)
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
            add_revids = backend_revids - ix_revids
            del_revids = ix_revids - backend_revids
            self._modify_index(index_all, self.schemas[ALL_REVS], self.wikiname, add_revids, 'add')
            self._modify_index(index_all, self.schemas[ALL_REVS], self.wikiname, del_revids, 'delete')

            backend_latest_revids = set(self._find_latest_revids(index_all))
        finally:
            index_all.close()
        index_latest = open_dir(index_dir, indexname=LATEST_REVS)
        try:
            # now update LATEST_REVS index:
            with index_latest.searcher() as searcher:
                ix_revids = set(doc[REVID] for doc in searcher.all_stored_fields())
            upd_revids = backend_latest_revids - ix_revids
            self._modify_index(index_latest, self.schemas[LATEST_REVS], self.wikiname, upd_revids, 'update')
            self._modify_index(index_latest, self.schemas[LATEST_REVS], self.wikiname, del_revids, 'delete')
        finally:
            index_latest.close()

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
                doc = hit.fields()
                item = self[doc[NAME]]
                if item.allows('read'):
                    yield doc

    def search_page(self, q, all_revs=False, pagenum=1, pagelen=10, **kw):
        """
        Same as search, but with paging support.
        """
        with self.get_index(all_revs).searcher() as searcher:
            # Note: callers must consume everything we yield, so the for loop
            # ends and the "with" is left to close the index files.
            for hit in searcher.search_page(q, pagenum, pagelen=pagelen, **kw):
                doc = hit.fields()
                item = self[doc[NAME]]
                if item.allows('read'):
                    yield doc

    def documents(self, all_revs=False, **kw):
        """
        Yield documents matching the kw args.
        """
        with self.get_index(all_revs).searcher() as searcher:
            # Note: callers must consume everything we yield, so the for loop
            # ends and the "with" is left to close the index files.
            if kw:
                for doc in searcher.documents(**kw):
                    item = self[doc[NAME]]
                    if item.allows('read'):
                        yield doc
            else: # XXX maybe this would make sense to be whoosh default behaviour for documents()?
                for doc in searcher.all_stored_fields():
                    item = self[doc[NAME]]
                    if item.allows('read'):
                        yield doc

    def document(self, all_revs=False, acl_check=True, **kw):
        """
        Return document matching the kw args.

        :param acl_check: check 'read' ACL if True
        """
        with self.get_index(all_revs).searcher() as searcher:
            doc = searcher.document(**kw)
            if doc and acl_check:
                item = self[doc[NAME]]
                if item.allows('read'):
                    return doc
            else:
                return doc

    def __getitem__(self, item_name):
        """
        Return item with <item_name> (may be a new or existing item).
        """
        return Item(self, item_name, user_name=self.user_name)

    def create_item(self, item_name):
        """
        Return item with <item_name> (must be a new item).
        """
        return Item.create(self, item_name, user_name=self.user_name)

    def existing_item(self, item_name):
        """
        Return item with <item_name> (must be an existing item).
        """
        return Item.existing(self, item_name, user_name=self.user_name)


class AccessDenied(Exception):
    """
    raised when a user is denied access to an Item or Revision by ACL.
    """


class Item(object):
    def __init__(self, indexer, item_name, user_name=None):
        self.indexer = indexer
        self.item_name = item_name
        self.user_name = user_name
        self.backend = self.indexer.backend
        # we need to switch off the acl check there to avoid endless recursion:
        self._current = self.indexer.document(all_revs=False, acl_check=False, name=item_name) or {}
        # keep it small, get rid of (big) stuff we do not need:
        self._current.pop(CONTENT, None) # indexable (preprocessed) content

    def _get_itemid(self):
        return self._current.get(ITEMID)
    def _set_itemid(self, value):
        self._current[ITEMID] = value
    itemid = property(_get_itemid, _set_itemid)

    @classmethod
    def create(cls, indexer, item_name, user_name=None):
        """
        Create a new item and return it, raise exception if it already exists.
        """
        item = cls(indexer, item_name, user_name)
        if not item:
            return item
        raise ItemAlreadyExists(item_name)
        
    @classmethod
    def existing(cls, indexer, item_name, user_name=None):
        """
        Get an existing item and return it, raise exception if it does not exist.
        """
        item = cls(indexer, item_name, user_name)
        if item:
            return item
        raise ItemDoesNotExist(item_name)

    def __nonzero__(self):
        """
        Item exists (== has at least one revision)?
        """
        return self.itemid is not None

    def allows(self, capability):
        # just a temporary hack to be able to test this without real ACL code
        # e.g. acl = "joe:read"  --> user joe may read
        if not self.indexer.acl_support:
            return True
        acl = self._current.get(ACL)
        user_name = self.user_name
        if acl is None or user_name is None:
            allow = True
        else:
            allow = "%s:%s" % (user_name, capability) in acl
        #print "item allows user '%s' to '%s' (acl: %s): %s" % (user_name, capability, acl, ["no", "yes"][allow])
        return allow

    def require(self, capability):
        if not self.allows(capability):
            raise AccessDenied("item does not allow user '%r' to '%r'" % (self.user_name, capability))


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
        self.require('read')
        return Revision(self, revid)

    def get_revision(self, revid):
        """
        Same as item[revid].
        """
        return self[revid]

    def store_revision(self, meta, data, overwrite=False):
        """
        Store a revision into the backend, write metadata and data to it.

        Usually this will be a new revision, either of an existing item or
        a new item. With overwrite mode, we can also store over existing
        revisions.

        :type meta: dict
        :type data: open file (file must be closed by caller)
        :param overwrite: if True, allow overwriting of existing revs.
        """
        self.require('write')
        if self.itemid is None:
            self.require('create')
            self.itemid = make_uuid()
        backend = self.backend
        if overwrite:
            self.require('overwrite')
        else:
            revid = meta.get(REVID)
            if revid is not None and revid in backend:
                raise AccessDenied('need overwrite flag to overwrite existing revisions')
        meta[ITEMID] = self.itemid
        revid = backend.store(meta, data)
        data.seek(0)  # rewind file
        self.indexer.index_revision(revid, meta, data)
        if not overwrite:
            self._current = self.indexer.document(all_revs=False, acl_check=False, revid=revid)
        return Revision(self, revid)

    def store_all_revisions(self, meta, data):
        """
        Store all revisions of this item.
        """
        for revid in self.iter_revs():
            meta[REVID] = revid
            self.store_revision(meta, data, overwrite=True)

    def destroy_revision(self, revid):
        """
        Check if revision with that revid exists (via index) -
        if yes, destroy revision with that revid.
        if no, raise RevisionDoesNotExistError.

        Note: "destroy" means: we delete the revision from the backend
        """
        self.require('destroy')
        self.backend.remove(revid)
        self.indexer.remove_revision(revid)
        
    def destroy_all_revisions(self):
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
        self.meta, self.data = self.backend.retrieve(self.revid) # raises KeyError if rev does not exist

    def close(self):
        self.data.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

