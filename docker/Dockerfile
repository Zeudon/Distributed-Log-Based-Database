# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Copy dependency files first for better layer caching
COPY go.mod ./
RUN go mod download

# Copy full source
COPY . .

# Build the node binary (statically linked, no CGO)
RUN CGO_ENABLED=0 GOOS=linux go build -o /node ./cmd/node

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /node /app/node

# Data directory (WAL, snapshots, Raft state) is expected to be mounted here
VOLUME ["/data"]

# Default RPC port
EXPOSE 7000

ENTRYPOINT ["/app/node"]
