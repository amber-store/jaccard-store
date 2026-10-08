-- A pack is the verified content of one root: a base pack holds the whole
-- key set, a patch pack names a base pack as its parent and holds what the
-- parent lacks.
CREATE TABLE packs (
  id             INTEGER PRIMARY KEY,
  root           BLOB    NOT NULL UNIQUE,
  parent_id      INTEGER REFERENCES packs(id),   -- NULL: a base pack
  data_key       TEXT    NOT NULL,
  index_key      TEXT    NOT NULL,
  links_key      TEXT,               -- base packs only
  data_size      INTEGER NOT NULL,   -- bytes in S3
  index_size     INTEGER NOT NULL,
  links_size     INTEGER NOT NULL,   -- 0 for a patch pack
  objects        INTEGER NOT NULL,   -- objects in the pack
  bytes          INTEGER NOT NULL,   -- their uncompressed bytes
  shared_objects INTEGER NOT NULL,   -- of the ref, held by the parent
  shared_bytes   INTEGER NOT NULL,   -- 0 for a base pack
  uploader       TEXT    NOT NULL,   -- endpoint ID
  uploaded_at    INTEGER NOT NULL,   -- unix seconds
  sketch         BLOB                -- base packs only: its keys, back to back
);
CREATE INDEX packs_parent ON packs(parent_id);

-- The sketch of every base pack, key by key: the index the nearest search
-- counts shared keys through.
CREATE TABLE sketch_keys (
  key     BLOB    NOT NULL,
  pack_id INTEGER NOT NULL REFERENCES packs(id) ON DELETE CASCADE,
  PRIMARY KEY (key, pack_id)
) WITHOUT ROWID;
-- Not in the design's schema. Deleting a pack cascades to its rows here, and
-- without this index SQLite finds them by reading the whole table: every
-- sketch key of every base pack, for each pack that is collected.
CREATE INDEX sketch_keys_pack ON sketch_keys(pack_id);

CREATE TABLE refs (
  name       TEXT    PRIMARY KEY,
  pack_id    INTEGER NOT NULL REFERENCES packs(id),
  updated_by TEXT    NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX refs_pack ON refs(pack_id);

-- An upload owns its three keys from the start, whether or not all of them
-- get written.
CREATE TABLE uploads (
  id             TEXT    PRIMARY KEY,
  name           TEXT    NOT NULL,
  root           BLOB    NOT NULL,
  parent_id      INTEGER REFERENCES packs(id),
  uploader       TEXT    NOT NULL,
  data_key       TEXT    NOT NULL,
  index_key      TEXT    NOT NULL,
  links_key      TEXT    NOT NULL,
  multipart_id   TEXT,
  data_size      INTEGER NOT NULL,
  objects        INTEGER NOT NULL,
  state          TEXT    NOT NULL,   -- 'pending' or 'verifying'
  issued_at      INTEGER NOT NULL,
  deadline       INTEGER NOT NULL
);

-- The queue every S3 delete goes through.
CREATE TABLE deletions (
  id           INTEGER PRIMARY KEY,
  object_key   TEXT    NOT NULL,
  multipart_id TEXT,                 -- set: abort this multipart upload
  not_before   INTEGER NOT NULL
);
