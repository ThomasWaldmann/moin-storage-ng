# ACL rights that are valid in moin2
SUPERUSER = 'superuser'
NOTEXTCHA = 'notextcha'
# rights that control access to specific functionality
ACL_RIGHTS_FUNCTIONS = [SUPERUSER, NOTEXTCHA, ]

CREATE = 'create'
READ = 'read'
WRITE = 'write'
DESTROY = 'destroy'
ADMIN = 'admin'
# rights that control access to operations on contents
ACL_RIGHTS_CONTENTS = [READ, WRITE, CREATE, ADMIN, DESTROY, ]

# metadata keys
NAME = "name"
NAME_OLD = "name_old"

# if an item is reverted, we store the revision number we used for reverting there:
REVERTED_TO = "reverted_to"

# some metadata key constants:
ACL = "acl"

# This says: I am a system item
IS_SYSITEM = "is_syspage"
# This says: original sysitem as contained in release: <release>
SYSITEM_VERSION = "syspage_version"

# keys for storing group and dict information
# group of user names, e.g. for ACLs:
USERGROUP = "usergroup"
# needs more precise name / use case:
SOMEDICT = "somedict"

CONTENTTYPE = "contenttype"
SIZE = "size"
LANGUAGE = "language"
ITEMLINKS = "itemlinks"
ITEMTRANSCLUSIONS = "itemtransclusions"
TAGS = "tags"

ACTION = "action"
ADDRESS = "address"
HOSTNAME = "hostname"
USERID = "userid"
MTIME = "mtime"
EXTRA = "extra"
COMMENT = "comment"

# we need a specific hash algorithm to store hashes of revision data into meta
# data. meta[HASH_ALGORITHM] = hash(rev_data, HASH_ALGORITHM)
# some backends may use this also for other purposes.
HASH_ALGORITHM = 'sha1'

# some field names for whoosh index schema / documents in index:
NAME_EXACT = "name_exact"
ITEMID = "itemid"
REVID = "revid"
DATAID = "dataid"
WIKINAME = "wikiname"
CONTENT = "content"

# stuff from user profiles / for whoosh index
EMAIL = "email"
OPENID = "openid"

