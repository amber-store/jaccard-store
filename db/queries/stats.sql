-- name: CountRefs :one
SELECT count(*) FROM refs;

-- name: CountUploads :one
SELECT count(*) FROM uploads;

-- name: CountDeletions :one
SELECT count(*) FROM deletions;

-- name: PackTotals :one
-- Every pack once: how many of each kind there are, what their objects come
-- to as they are, what the bucket holds of them, and how many of them were
-- recorded without being verified.
SELECT count(parent_id) AS patch_packs,
       count(*) - count(parent_id) AS base_packs,
       CAST(COALESCE(sum(bytes), 0) AS INTEGER) AS pack_bytes,
       CAST(COALESCE(sum(data_size), 0) AS INTEGER) AS data_bytes,
       CAST(COALESCE(sum(index_size + links_size), 0) AS INTEGER) AS index_bytes,
       CAST(COALESCE(sum(1 - verified), 0) AS INTEGER) AS unverified_packs
FROM packs;

-- name: UnreferencedTotals :one
-- The packs no ref points at. A pack without a ref is there because
-- something leans on it: patch packs, or an upload that is to become one.
SELECT count(*) AS packs,
       CAST(COALESCE(sum(p.bytes), 0) AS INTEGER) AS pack_bytes,
       CAST(COALESCE(sum(p.data_size), 0) AS INTEGER) AS data_bytes
FROM packs AS p
WHERE NOT EXISTS (SELECT 1 FROM refs AS r WHERE r.pack_id = p.id);

-- name: RefTotals :one
-- Over refs, not packs: two refs on one pack count it twice, as two
-- directories would hold its content twice.
SELECT CAST(COALESCE(sum(p.unpacked), 0) AS INTEGER) AS unpacked_bytes,
       CAST(COALESCE(sum(p.bytes + p.shared_bytes), 0) AS INTEGER) AS object_bytes
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id;
