//go:build deps

// Package tools pins the module's dependencies while its packages are being
// written. It is removed once every package imports what it needs.
package tools

import (
	_ "github.com/amber-store/core/amberpack"
	_ "github.com/amber-store/core/commit"
	_ "github.com/amber-store/core/fstree"
	_ "github.com/amber-store/core/gc"
	_ "github.com/amber-store/core/ingest"
	_ "github.com/amber-store/core/key"
	_ "github.com/amber-store/core/packstore"
	_ "github.com/amber-store/core/reference"
	_ "github.com/amber-store/core/refstore"
	_ "github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	_ "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/credentials"
	_ "github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/fxamacker/cbor/v2"
	_ "github.com/johannesboyne/gofakes3"
	_ "github.com/johannesboyne/gofakes3/backend/s3mem"
	_ "github.com/klauspost/compress/zstd"
	_ "github.com/tmc/go-iroh/dns"
	_ "github.com/tmc/go-iroh/iroh"
	_ "github.com/tmc/go-iroh/iroh/mdns"
	_ "github.com/tmc/go-iroh/key"
	_ "github.com/tmc/go-iroh/netaddr"
	_ "github.com/tmc/go-iroh/relay"
	_ "github.com/urfave/cli/v2"
	_ "github.com/zeebo/blake3"
	_ "golang.org/x/sync/errgroup"
	_ "modernc.org/sqlite"
)
