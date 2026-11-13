FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    make \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /duckbeetle/
COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./
ENV CGO_ENABLED=1
RUN make build

FROM debian:bookworm-slim AS deploy
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /duckbeetle/duckbeetle ./
COPY --from=builder /duckbeetle/sql ./sql
RUN mkdir -p /opt/config

VOLUME ["/opt/config/"]

CMD ["./duckbeetle"]
