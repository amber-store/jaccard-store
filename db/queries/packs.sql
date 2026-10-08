-- The reads of a pack leave its sketch out: it is some kilobytes, and only
-- the nearest search wants it.

-- name: InsertPack :one
INSERT INTO packs (
    root, parent_id, data_key, index_key, links_key, data_size, index_size,
    links_size, objects, bytes, shared_objects, shared_bytes, uploader,
    uploaded_at, sketch, unpacked, verified
) VALUES (
    sqlc.arg(root), sqlc.narg(parent_id), sqlc.arg(data_key),
    sqlc.arg(index_key), sqlc.narg(links_key), sqlc.arg(data_size),
    sqlc.arg(index_size), sqlc.arg(links_size), sqlc.arg(objects),
    sqlc.arg(bytes), sqlc.arg(shared_objects), sqlc.arg(shared_bytes),
    sqlc.arg(uploader), sqlc.arg(uploaded_at), sqlc.narg(sketch),
    sqlc.arg(unpacked), sqlc.arg(verified)
)
RETURNING id;

-- name: PackByID :one
SELECT id, root, parent_id, data_key, index_key, links_key, data_size,
       index_size, links_size, objects, bytes, shared_objects, shared_bytes,
       uploader, uploaded_at, verified
FROM packs
WHERE id = sqlc.arg(id);

-- name: PackByRoot :one
SELECT id, root, parent_id, data_key, index_key, links_key, data_size,
       index_size, links_size, objects, bytes, shared_objects, shared_bytes,
       uploader, uploaded_at, verified
FROM packs
WHERE root = sqlc.arg(root);

-- name: InsertSketchKey :exec
INSERT INTO sketch_keys (key, pack_id)
VALUES (sqlc.arg(key), sqlc.arg(pack_id));

-- name: SharingPacks :many
-- The 16 packs that share the most of the given keys with their sketches,
-- and how many each shares. Blobs compare as their bytes, so the lower root
-- wins a tie. The limit is written out: sqlc numbers its parameters, and a
-- numbered one after the list of keys would be bound to one of the keys.
SELECT p.id, p.root, p.sketch, count(*) AS shared
FROM sketch_keys AS s
JOIN packs AS p ON p.id = s.pack_id
WHERE s.key IN (sqlc.slice('keys'))
GROUP BY p.id
ORDER BY count(*) DESC, p.root ASC
LIMIT 16;

-- name: DeletePackIfDead :many
-- The pack with the given ID, if nothing holds it: no row otherwise. A patch
-- pack that goes can leave its base without a holder, so the parent comes
-- back to be looked at next.
DELETE FROM packs
WHERE packs.id = sqlc.arg(id)
  AND NOT EXISTS (SELECT 1 FROM refs AS r WHERE r.pack_id = packs.id)
  AND NOT EXISTS (SELECT 1 FROM packs AS c WHERE c.parent_id = packs.id)
  AND NOT EXISTS (SELECT 1 FROM uploads AS u WHERE u.parent_id = packs.id)
RETURNING parent_id, data_key, index_key, links_key;

-- name: ListPacks :many
SELECT p.id, p.root, p.parent_id, p.data_key, p.index_key, p.links_key,
       p.data_size, p.index_size, p.links_size, p.objects, p.bytes,
       p.shared_objects, p.shared_bytes, p.uploader, p.uploaded_at, p.verified,
       parent.root AS parent_root,
       (SELECT count(*) FROM refs AS r WHERE r.pack_id = p.id) AS refs,
       (SELECT count(*) FROM packs AS c WHERE c.parent_id = p.id) AS children
FROM packs AS p
LEFT JOIN packs AS parent ON parent.id = p.parent_id
WHERE p.id > sqlc.arg(after_id)
ORDER BY p.id
LIMIT sqlc.arg(n);

-- name: ChildRoots :many
SELECT root FROM packs WHERE parent_id = sqlc.arg(parent_id) ORDER BY root;

-- name: TopPacksByRefs :many
-- The packs the most refs point at, the most first and among equals the
-- older. largest_share is the most of a pack's bytes that one of the patch
-- packs leaning on it uses, 0 when none leans on it.
SELECT p.id, p.root, p.parent_id, p.data_key, p.index_key, p.links_key,
       p.data_size, p.index_size, p.links_size, p.objects, p.bytes,
       p.shared_objects, p.shared_bytes, p.uploader, p.uploaded_at, p.verified,
       parent.root AS parent_root,
       count(*) AS refs,
       (SELECT count(*) FROM packs AS c WHERE c.parent_id = p.id) AS children,
       CAST((SELECT COALESCE(max(c.shared_bytes), 0) FROM packs AS c WHERE c.parent_id = p.id) AS INTEGER) AS largest_share
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id
LEFT JOIN packs AS parent ON parent.id = p.parent_id
GROUP BY p.id
ORDER BY count(*) DESC, p.id
LIMIT sqlc.arg(n);

-- name: TopPacksByChildren :many
-- The packs the most patch packs lean on, the most first and among equals
-- the older. Only a base pack is leaned on, so none of them has a parent.
SELECT p.id, p.root, p.parent_id, p.data_key, p.index_key, p.links_key,
       p.data_size, p.index_size, p.links_size, p.objects, p.bytes,
       p.shared_objects, p.shared_bytes, p.uploader, p.uploaded_at, p.verified,
       (SELECT count(*) FROM refs AS r WHERE r.pack_id = p.id) AS refs,
       count(*) AS children,
       CAST(max(c.shared_bytes) AS INTEGER) AS largest_share
FROM packs AS c
JOIN packs AS p ON p.id = c.parent_id
GROUP BY p.id
ORDER BY count(*) DESC, p.id
LIMIT sqlc.arg(n);
