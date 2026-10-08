-- The reads of a pack leave its sketch out: it is some kilobytes, and only
-- the nearest search wants it.

-- name: InsertPack :one
INSERT INTO packs (
    root, parent_id, data_key, index_key, links_key, data_size, index_size,
    links_size, objects, bytes, shared_objects, shared_bytes, uploader,
    uploaded_at, sketch
) VALUES (
    sqlc.arg(root), sqlc.narg(parent_id), sqlc.arg(data_key),
    sqlc.arg(index_key), sqlc.narg(links_key), sqlc.arg(data_size),
    sqlc.arg(index_size), sqlc.arg(links_size), sqlc.arg(objects),
    sqlc.arg(bytes), sqlc.arg(shared_objects), sqlc.arg(shared_bytes),
    sqlc.arg(uploader), sqlc.arg(uploaded_at), sqlc.narg(sketch)
)
RETURNING id;

-- name: PackByID :one
SELECT id, root, parent_id, data_key, index_key, links_key, data_size,
       index_size, links_size, objects, bytes, shared_objects, shared_bytes,
       uploader, uploaded_at
FROM packs
WHERE id = sqlc.arg(id);

-- name: PackByRoot :one
SELECT id, root, parent_id, data_key, index_key, links_key, data_size,
       index_size, links_size, objects, bytes, shared_objects, shared_bytes,
       uploader, uploaded_at
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

-- name: DeleteDeadPacks :many
-- One pass of collection: the packs nothing holds. Deleting a patch pack can
-- leave its base without a holder, which the next pass finds.
DELETE FROM packs
WHERE NOT EXISTS (SELECT 1 FROM refs AS r WHERE r.pack_id = packs.id)
  AND NOT EXISTS (SELECT 1 FROM packs AS c WHERE c.parent_id = packs.id)
  AND NOT EXISTS (SELECT 1 FROM uploads AS u WHERE u.parent_id = packs.id)
RETURNING data_key, index_key, links_key;

-- name: ListPacks :many
SELECT p.id, p.root, p.parent_id, p.data_key, p.index_key, p.links_key,
       p.data_size, p.index_size, p.links_size, p.objects, p.bytes,
       p.shared_objects, p.shared_bytes, p.uploader, p.uploaded_at,
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
