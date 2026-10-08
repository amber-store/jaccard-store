# jaccard-store

A server that keeps references as pack files in an S3 bucket, and a client
that pushes references to it and pulls them back. Objects and keys are
[Amber-Store Core](https://github.com/amber-store/core)'s.

Every reference gets at most one pack, named by its root key:

- a **base pack** holds every object of the reference;
- a **patch pack** names one base pack as its parent and holds only the
  objects the parent lacks.

A push sends the server a sketch of the reference's key set (its 256 lowest
keys). The server answers with the three base packs whose sketches are
nearest by estimated Jaccard distance, and the client uploads a patch pack
against the best of them, or a base pack if none is near enough. This is the
scheme of
[jaccard-store-experiment](https://github.com/amber-store/jaccard-store-experiment),
which measured it capturing 86 % of what a fully deduplicating store saves.

The server never carries pack bytes. Clients move them between themselves
and the bucket through pre-signed URLs; the server downloads an upload,
walks it from the root, checks every object against its key, and only then
lets a reference point at it. A pack that fails is deleted. An upload that
is not committed within an hour is cleaned out of the bucket. A pack that no
reference and no patch pack needs any more is collected.

The design is in
[`docs/superpowers/specs/2026-10-08-jaccard-store-design.md`](docs/superpowers/specs/2026-10-08-jaccard-store-design.md).

## Server

```sh
jaccard-stored --data /var/lib/jaccard --s3-bucket my-packs
```

The data directory holds the server's key (`server.key`, created on first
run), its database (`store.sqlite`) and scratch space for the packs it is
verifying. S3 credentials come from the AWS SDK's default chain
(`AWS_ACCESS_KEY_ID` and the rest, a profile, or a role).

At start the server logs its **endpoint ID**. That ID is all a client needs:
the server publishes its addresses through iroh's relay infrastructure and
answers mDNS on the local network.

| flag | environment | default |
| --- | --- | --- |
| `--data DIR` | `JACCARD_DATA` | required |
| `--s3-bucket NAME` | `JACCARD_S3_BUCKET` | required |
| `--s3-prefix P` | `JACCARD_S3_PREFIX` | none |
| `--s3-endpoint URL` | `JACCARD_S3_ENDPOINT` | AWS |
| `--s3-region R` | `JACCARD_S3_REGION` | the SDK's |
| `--s3-path-style` | `JACCARD_S3_PATH_STYLE` | off |
| `--admin-addr ADDR` | `JACCARD_ADMIN_ADDR` | `127.0.0.1:8080` |
| `--upload-timeout D` | `JACCARD_UPLOAD_TIMEOUT` | `1h` |
| `--url-ttl D` | `JACCARD_URL_TTL` | `1h` |
| `--part-size N` | `JACCARD_PART_SIZE` | `64MiB` |
| `--verify-jobs N` | `JACCARD_VERIFY_JOBS` | `2` |
| `--max-pack-size N` | `JACCARD_MAX_PACK_SIZE` | `16GiB` |

A flag wins over its variable. The two durations are lifetimes of pre-signed
URLs, between one second and seven days. The server needs scratch space for
`--max-pack-size` times `--verify-jobs`: a pack is decompressed there to be
verified, and the limit is what keeps a few megabytes of upload from
announcing terabytes.

### Admin page

`http://127.0.0.1:8080` shows the number of references and packs, the bytes
in the bucket, the deduplication rate, every reference with how much of it
its parent holds and how much of the parent it cannot reach, and every pack
with who uploaded it and when. The figures are the server's own, computed
from the packs it verified. The page is read-only and has no
authentication, which is why it listens on loopback. On loopback it answers
only to a loopback address or `localhost` as the host name.

## Client

The client works on a store directory of core's own CLI (a packstore and a
refstore). Ingest and restore with `amber-store`; push and pull with
`jaccard-store`.

```sh
export JACCARD_STORE=./st JACCARD_SERVER=<endpoint id>

amber-store --store ./st ingest --ref snap ./some/dir
jaccard-store push snap                  # or: push --as releases/1.0 snap
jaccard-store ls
jaccard-store pull --as copy releases/1.0
jaccard-store rm releases/1.0
```

Options come before the argument, as with core's CLI.

| flag | environment | default |
| --- | --- | --- |
| `--store DIR` | `JACCARD_STORE`, then `AMBER_STORE` | required for push and pull |
| `--server ENDPOINT_ID` | `JACCARD_SERVER` | required |
| `--key FILE` | `JACCARD_KEY` | `jaccard-store/client.key` in the user's configuration directory |
| `push --min-dedup F` | `JACCARD_MIN_DEDUP` | `0.5` |

The key file is created on first use. Its endpoint ID is what the server
records as the uploader of a pack.

A pull fetches the reference's own pack, checks whether the store now holds
everything, and fetches the parent only if it does not: a store that
already has the parent's content never downloads it again.

## Limits

- **Access is open.** Whoever knows the endpoint ID can push, pull, list and
  delete, and so can fill the bucket.
- **The bucket's service must honor conditional writes** (`If-None-Match: *`
  on PUT, as AWS S3 does). Upload URLs are signed with that condition so
  that an object the server has verified cannot be replaced afterwards.
- A reference whose pack is larger than `--max-pack-size` uncompressed
  cannot be pushed.
- **The database is the only record** of the references and of which pack
  leans on which. Back it up; the bucket alone does not describe the store.
- A verification needs scratch space for the uncompressed pack.
- A pull that needs the parent downloads all of it.
- One server per bucket prefix.

## Development

The Nix build is [gonixgo](https://github.com/draganm/gonixgo)'s: one
derivation per Go package, and no Nix file to update when `go.mod` changes.
gonixgo runs a program while Nix evaluates, which Nix allows only when
asked, so every command that touches the flake's package takes an option.
The dev shell carries the built commands and needs it too:

```sh
nix build --option allow-unsafe-native-code-during-evaluation true
./result/bin/jaccard-stored --help

nix develop --option allow-unsafe-native-code-during-evaluation true
```

In the dev shell are `go`, `sqlc`, and `jaccard-stored` and `jaccard-store`
as built from the tree. With direnv, `.envrc` passes the option and reloads
the shell, rebuilding the two commands, when the sources change. Nix sees
only the files git tracks, so `git add` new ones.

```sh
go test ./...
(cd db && go generate)   # after changing db/queries or db/migrations
```

The end-to-end tests in `e2e/` run a server and its clients in one process,
over real iroh endpoints on loopback and an S3 in memory. Nothing in the
test suite needs the network, except `TestOnline` in `node/`, which runs
only with `JACCARD_TEST_ONLINE=1`.

## License

Licensed under the GNU Lesser General Public License, version 3 only
(`LGPL-3.0-only`). See [`LICENSE`](LICENSE) for the LGPL terms and
[`COPYING`](COPYING) for the GPL terms incorporated by the LGPL.
