-- A server can be run without verification. It then records a pack on the
-- word of the client that uploaded it: it has seen that the pack's objects
-- are in the bucket and that its index parses, and nothing of its data. Such
-- a pack is marked, so that what was never walked can be told from what was.
-- Every pack that is there already was verified.
ALTER TABLE packs ADD COLUMN verified INTEGER NOT NULL DEFAULT 1;

-- What the client says its ref shares with the parent it names. A server
-- that verifies measures this itself, by the parent's links. Where it cannot
-- (it does not verify, or the parent was never verified and so has no
-- links) this is the figure it records.
ALTER TABLE uploads ADD COLUMN shared_objects INTEGER NOT NULL DEFAULT 0;
ALTER TABLE uploads ADD COLUMN shared_bytes INTEGER NOT NULL DEFAULT 0;
