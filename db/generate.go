package db

// The queries in queries/ are compiled against the schema in migrations/ into
// dbq. sqlc comes from the Nix dev shell; the output is committed, so
// building needs no sqlc.
//go:generate sqlc generate -f ../sqlc.yaml
