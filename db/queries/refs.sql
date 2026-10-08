-- name: UpsertRef :exec
INSERT INTO refs (name, pack_id, updated_by, updated_at)
VALUES (sqlc.arg(name), sqlc.arg(pack_id), sqlc.arg(updated_by), sqlc.arg(updated_at))
ON CONFLICT (name) DO UPDATE SET
    pack_id = excluded.pack_id,
    updated_by = excluded.updated_by,
    updated_at = excluded.updated_at;

-- name: RefByName :one
SELECT r.name, r.pack_id, r.updated_by, r.updated_at, p.root
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id
WHERE r.name = sqlc.arg(name);

-- name: ListRefsFrom :many
-- The names from lower on that come after the given one. A prefix is listed
-- as a range of names, never with LIKE: its characters are literal.
SELECT r.name, r.pack_id, r.updated_by, r.updated_at, p.root
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id
WHERE r.name >= sqlc.arg(lower) AND r.name > sqlc.arg(after)
ORDER BY r.name
LIMIT sqlc.arg(n);

-- name: ListRefsBetween :many
SELECT r.name, r.pack_id, r.updated_by, r.updated_at, p.root
FROM refs AS r
JOIN packs AS p ON p.id = r.pack_id
WHERE r.name >= sqlc.arg(lower) AND r.name > sqlc.arg(after)
  AND r.name < sqlc.arg(upper)
ORDER BY r.name
LIMIT sqlc.arg(n);

-- name: DeleteRef :execrows
DELETE FROM refs WHERE name = sqlc.arg(name);

-- name: RefNamesOfPack :many
SELECT name FROM refs WHERE pack_id = sqlc.arg(pack_id) ORDER BY name;
