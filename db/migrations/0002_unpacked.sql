-- What a pack's root comes to when it is unpacked: the size of the tree, as
-- the root's key records it in its length field. It is a figure of the key
-- and nothing the server measured. It is kept beside the root so that the
-- figures of the store can add it up without reading every key.
--
-- The packs that are there already get theirs from their roots in the same
-- transaction: that is the part of this migration SQL cannot do (db.go).
ALTER TABLE packs ADD COLUMN unpacked INTEGER NOT NULL DEFAULT 0;
