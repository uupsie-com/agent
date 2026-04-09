FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod tidy && CGO_ENABLED=0 go build -o /agent ./cmd/agent

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /agent /agent
ENTRYPOINT ["/agent"]
