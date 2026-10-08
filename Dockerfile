# The jaccard-store image: the server, jaccard-stored, as the entrypoint, with
# the client, jaccard-store, beside it. The server is configured through the
# JACCARD_* variables and the AWS_* variables of the SDK; see the README.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/ ./cmd/jaccard-stored ./cmd/jaccard-store

FROM alpine:3.23
LABEL org.opencontainers.image.source=https://github.com/amber-store/jaccard-store
LABEL org.opencontainers.image.description="jaccard-store: references as base and patch packs in S3, served over iroh"
LABEL org.opencontainers.image.licenses=LGPL-3.0-only
# The server reads uploads from anybody, so it does not run as root. /data
# is its own: a named volume takes that ownership over; a directory of the
# host mounted there has to be writable for user 10001.
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 jaccard \
    && mkdir /data && chown jaccard:jaccard /data
COPY --from=build /out/jaccard-stored /out/jaccard-store /usr/local/bin/
USER jaccard
# The key, the database and the scratch space live in /data. The admin page
# listens on every address of the container, because loopback inside a
# container reaches nobody. It has no authentication: publish its port on
# the host's loopback (-p 127.0.0.1:8080:8080), not on every interface.
ENV JACCARD_DATA=/data JACCARD_ADMIN_ADDR=0.0.0.0:8080
VOLUME ["/data"]
EXPOSE 8080/tcp
ENTRYPOINT ["/usr/local/bin/jaccard-stored"]
