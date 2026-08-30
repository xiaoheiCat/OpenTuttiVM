# open-tutti-server: the bare collaboration server. It never runs agent
# code; agents always run in each user's own Docker Desktop VM.
FROM golang:1.24.5-alpine AS build
WORKDIR /src

# Workspace modules first for layer caching.
COPY go.work.sum* ./
COPY packages/workspace/vm-protocol/go.mod packages/workspace/vm-protocol/
COPY packages/workspace/vm-cas/go.mod packages/workspace/vm-cas/
COPY packages/workspace/vm-sync/go.mod packages/workspace/vm-sync/
COPY packages/agent/borrow/go.mod packages/agent/borrow/
COPY services/open-tutti-server/go.mod services/open-tutti-server/go.sum services/open-tutti-server/
RUN go mod download -C services/open-tutti-server

COPY packages/workspace packages/workspace
COPY packages/agent/borrow packages/agent/borrow
COPY services/open-tutti-server services/open-tutti-server
# GOWORK=off: the staged image contains only this server's module graph;
# the repository-wide go.work references modules that are not copied in.
# Build from inside the module directory — with GOWORK=off the /src root
# has no go.mod, so package-path builds cannot resolve.
RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go -C services/open-tutti-server build \
    -trimpath -ldflags="-s -w" -o /out/open-tutti-server .

FROM alpine:3.21
RUN addgroup -S tutti && adduser -S tutti -G tutti \
    && mkdir -p /data && chown -R tutti:tutti /data
COPY --from=build /out/open-tutti-server /usr/local/bin/open-tutti-server
USER tutti
ENV OPEN_TUTTI_LISTEN_ADDR=0.0.0.0:8080 \
    OPEN_TUTTI_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/open-tutti-server"]
