-- name: InsertUpload :exec
INSERT INTO uploads (
    id, name, root, parent_id, uploader, data_key, index_key, links_key,
    multipart_id, data_size, objects, state, issued_at, deadline
) VALUES (
    sqlc.arg(id), sqlc.arg(name), sqlc.arg(root), sqlc.narg(parent_id),
    sqlc.arg(uploader), sqlc.arg(data_key), sqlc.arg(index_key),
    sqlc.arg(links_key), sqlc.narg(multipart_id), sqlc.arg(data_size),
    sqlc.arg(objects), sqlc.arg(state), sqlc.arg(issued_at), sqlc.arg(deadline)
);

-- name: UploadByID :one
SELECT * FROM uploads WHERE id = sqlc.arg(id);

-- name: ListUploads :many
SELECT * FROM uploads ORDER BY issued_at, id;

-- name: ExpiredUploads :many
-- An upload being verified is not expired under its verifier.
SELECT * FROM uploads
WHERE state = 'pending' AND deadline < sqlc.arg(now)
ORDER BY id;

-- name: SetUploadState :execrows
UPDATE uploads SET state = sqlc.arg(state) WHERE id = sqlc.arg(id);

-- name: ResetVerifying :exec
UPDATE uploads SET state = 'pending' WHERE state = 'verifying';

-- name: DeleteUpload :exec
DELETE FROM uploads WHERE id = sqlc.arg(id);

-- name: InsertDeletion :exec
INSERT INTO deletions (object_key, multipart_id, not_before)
VALUES (sqlc.arg(object_key), sqlc.narg(multipart_id), sqlc.arg(not_before));

-- name: DueDeletions :many
-- In the order they were queued, so the abort of a multipart upload comes
-- before the delete of the object it may have completed.
SELECT id, object_key, multipart_id
FROM deletions
WHERE not_before <= sqlc.arg(now)
ORDER BY id
LIMIT sqlc.arg(n);

-- name: DeleteDeletion :exec
DELETE FROM deletions WHERE id = sqlc.arg(id);
