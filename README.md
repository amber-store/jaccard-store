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
against the best of them, or a base pack if none is near enough.

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
| `--bind IP:PORT` | `JACCARD_BIND` | every address, a port the system picks |

A flag wins over its variable. `--bind` names the UDP address of the iroh
endpoint, so that a firewall rule can be written for its port;
`0.0.0.0:4435` is every IPv4 address of the machine. The server advertises
the addresses that socket can be reached at, and clients dial them directly
instead of going through a relay. The two durations are lifetimes of pre-signed
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

## Docker

A release publishes `ghcr.io/amber-store/jaccard-store` for amd64 and arm64.
Its entrypoint is `jaccard-stored`; the client, `jaccard-store`, is in the
image as well.

```sh
docker run -d --name jaccard \
  -v jaccard-data:/data \
  -p 127.0.0.1:8080:8080 \
  -e JACCARD_S3_BUCKET=my-packs \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_REGION=eu-central-1 \
  ghcr.io/amber-store/jaccard-store

docker logs jaccard 2>&1 | grep endpoint=   # the ID clients need
```

- `/data` holds the server's key, its database and its scratch space. Keep
  it in a volume: the key is the server's identity, and the database is the
  only record of the store. The server runs as user 10001; a directory of
  the host mounted at `/data` has to be writable for that user.
- The admin page listens on every address inside the container, since
  loopback there reaches nobody. It has no authentication, so publish its
  port on the host's loopback, as above, and not on every interface.
- No port is published for iroh: the server binds a UDP port of its own
  choosing, and clients reach a container through iroh's relays. Requests
  are small and pack bytes do not pass through the server, so that costs
  little. To be dialed directly, run the container on the host network
  (`--network host` on Linux) and pin the port with `JACCARD_BIND`, such as
  `0.0.0.0:4435`. On the host network also set `JACCARD_ADMIN_ADDR` to an
  address that is not public: the image's default is every address.
- The S3 endpoint the server is given is the one in the URLs it hands to
  clients. It has to be reachable for them under the same name.

## Releases

Pushing a tag `v*` runs `.github/workflows/release.yml`: it builds the image
for both platforms, pushes it as `:<tag>` and `:latest`, and creates a
GitHub release for the tag with notes generated from what was merged since
the last one. The two commands of a release report the tag with `--version`.

```sh
git tag v0.1.0 && git push origin v0.1.0
```

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

A directory goes to the server and comes back without a store of your own:

```sh
export JACCARD_SERVER=<endpoint id>

jaccard-store push-dir ./some/dir releases/1.0
jaccard-store pull-dir releases/1.0 ./copy
```

`push-dir` imports the directory into a store it makes in a temporary
directory, pushes it from there and removes the store again; `pull-dir`
fetches into such a store, extracts the tree and removes the store. Both
remove it however they end, an interrupt included.

- `push-dir` reads the directory as `amber-store ingest` does, with the
  same chunking, so the same directory is the same root whichever way it
  was pushed, and a second version becomes a patch pack of the first. It
  honors `.amberignore` files unless `--no-ignore` is given.
- `pull-dir` extracts to a directory that is not there yet, or is empty.
  The tree is written beside it and moved into place when it is whole, so
  an extraction that fails or is interrupted leaves nothing behind.
  Permissions, modification times, symbolic links and extended attributes
  are restored; ownership only when running as root.
- A reference whose root is a single file (`push` can make one) is refused
  by `pull-dir` before anything is fetched.
- The temporary store holds the whole content once more, uncompressed, and
  a push builds its pack beside it: `--temp-dir` puts it on a disk that has
  the room.

Options come before the arguments, as with core's CLI.

| flag | environment | default |
| --- | --- | --- |
| `--store DIR` | `JACCARD_STORE`, then `AMBER_STORE` | required for push and pull |
| `--server ENDPOINT_ID` | `JACCARD_SERVER` | required |
| `--key FILE` | `JACCARD_KEY` | `jaccard-store/client.key` in the user's configuration directory |
| `push --min-dedup F`, `push-dir --min-dedup F` | `JACCARD_MIN_DEDUP` | `0.5` |
| `push-dir --no-ignore` | `JACCARD_NO_IGNORE` | `.amberignore` files are honored |
| `push-dir --temp-dir DIR`, `pull-dir --temp-dir DIR` | `JACCARD_TEMP_DIR` | the system's temporary directory |
| `--no-progress` on push, pull, push-dir and pull-dir | `JACCARD_NO_PROGRESS` | progress is shown |

The key file is created on first use. Its endpoint ID is what the server
records as the uploader of a pack.

### Progress

A push and a pull say on standard error what they are doing. Every step
leaves a line with the time it took and what came of it; the step that is
running shows its elapsed time and, when it knows how much there is to do,
a bar, the percentage, the amounts, the rate and the time left:

```
✓ connecting to the server     1.1s
✓ reading the tree             0.0s  2,604 objects
✓ finding nearby packs         0.1s  1 on the server
✓ comparing nearby packs       0.2s  it holds 152.78 MiB of the reference
✓ packing                      0.0s  patch pack of 523 objects, 38.16 MiB → 38.17 MiB
⠹ uploading                      1s  ███████████░░░░░░░░░░░░░░░░   41.1%  15.70 / 38.19 MiB   12.64 MiB/s  eta 2s
```

The rate is that of the last ten seconds, and the time left follows from
it. A narrow terminal loses the amounts first, then the rate, then the
bar. The server's verification of an upload has no bar: the server says
nothing until it is through, so only the time it has taken is known.

Where standard error is not a terminal, every step is a plain line when it
ends, and a step that runs long reports every five seconds:

```
uploading: 45.0%, 55.88 MiB of 124.11 MiB, 9.68 MiB/s, elapsed 6s, eta 7s
uploading: 124.11 MiB in 2 parts (9.4s)
```

`push-dir` and `pull-dir` show their own steps the same way, around those
of the push and the pull: scanning the directory and importing it before,
reading the tree and extracting it after, and cleaning up at the end. A
step that fails keeps its mark while the temporary store is removed.

The result of the command goes to standard output as before.
`--no-progress` leaves standard error to the errors; `NO_COLOR` keeps the
marks uncoloured.

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

The default shell cannot be entered while the Go module does not resolve,
because building the commands is part of it: after a `go get` that left
`go.sum` behind, for example. `nix develop .#bare` has `go` and `sqlc`
without the commands and needs no option; run `go mod tidy` there.

### End-to-end tests

The tests in `e2e/` run a server and its clients in one process, over real
iroh endpoints on loopback, against two kinds of bucket:

- an S3 in memory, which is always there and checks no signature;
- [RustFS](https://github.com/rustfs/rustfs) in a container, started with
  [testcontainers](https://golang.testcontainers.org/). It checks what S3
  checks: the signature of every request, the headers that were signed,
  when a URL expires, and that no part of a multipart upload but the last
  is under 5 MiB.

Every scenario runs against both. Three tests run against the container
alone, because they are about what only a real service enforces: that an
upload URL cannot be used without its create-only condition or after its
deadline, and that the URL completing a multipart upload, which is signed
by hand, completes that upload and no other.

The container tests need Docker and pull `rustfs/rustfs:1.0.1` once. They
are skipped where there is no Docker and with `go test -short`.

| variable | meaning |
| --- | --- |
| `JACCARD_TEST_RUSTFS_IMAGE` | another RustFS image than `rustfs/rustfs:1.0.1` |
| `JACCARD_TEST_MINIO_IMAGE` | run everything against MinIO as well, from this image |
| `JACCARD_TEST_S3_ENDPOINT`, `JACCARD_TEST_S3_BUCKET` | run everything against a service that exists already, in this bucket, as well |
| `JACCARD_TEST_S3_ACCESS_KEY_ID`, `JACCARD_TEST_S3_SECRET_ACCESS_KEY` | its credentials |
| `JACCARD_TEST_S3_REGION`, `JACCARD_TEST_S3_PATH_STYLE` | its region (default `us-east-1`; `auto` on R2) and `false` for virtual-hosted addressing |
| `JACCARD_TEST_ONLINE=1` | run `TestOnline` in `node/`, which reaches iroh's relays |

Against a service that exists already, such as a bucket on AWS or on
Cloudflare R2, every test works under a prefix of its own,
`jaccard-store-test/`, and removes what it left there.

MinIO is not run by default because its own images have left the public
registries, so there is none to pin. A build by somebody else works, for
example `JACCARD_TEST_MINIO_IMAGE=cgr.dev/chainguard/minio:latest`.

### Continuous integration

`.github/workflows/test.yml` runs on every push and pull request: `gofmt`,
`go vet`, `sqlc diff` (the generated queries are what the SQL says), the
tests, and the tests again under the race detector. The runner has Docker,
so the container tests run there too.

## License

Licensed under the GNU Lesser General Public License, version 3 only
(`LGPL-3.0-only`). See [`LICENSE`](LICENSE) for the LGPL terms and
[`COPYING`](COPYING) for the GPL terms incorporated by the LGPL.
