-- name: CountRefs :one
SELECT count(*) FROM refs;

-- name: CountUploads :one
SELECT count(*) FROM uploads;

-- name: CountDeletions :one
SELECT count(*) FROM deletions;

-- name: PackTotals :one
SELECT count(parent_id) AS patch_packs,
       count(*) - count(parent_id) AS base_packs,
       CAST(COALESCE(sum(data_size + index_size + links_size), 0) AS INTEGER) AS s3_bytes,
       CAST(COALESCE(sum(data_size), 0) AS INTEGER) AS data_bytes,
       CAST(COALESCE(sum(bytes), 0) AS INTEGER) AS stored_bytes
FROM packs;

-- name: LogicalBytes :one
-- Over refs, not packs: two refs on one pack count it twice.
SELECT CAST(COALESCE(sum(p.bytes + p.shared_bytes), 0) AS INTEGER)
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id;
