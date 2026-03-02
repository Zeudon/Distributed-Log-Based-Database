# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

# CMD_PATH selects which binary to build: ./cmd/node (default) or ./cmd/gateway
ARG CMD_PATH=./cmd/node

WORKDIR /src

# Copy dependency files first for better layer caching
COPY go.mod ./
RUN go mod download

# Copy full source
COPY . .

# Build the selected binary (statically linked, no CGO)
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ${CMD_PATH}

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /server /app/server

# Data directory (WAL, snapshots, Raft state) is expected to be mounted here
VOLUME ["/data"]

# Default RPC port (overridden by HTTP_PORT / GATEWAY_PORT for other binaries)
EXPOSE 7000

ENTRYPOINT ["/app/server"]
