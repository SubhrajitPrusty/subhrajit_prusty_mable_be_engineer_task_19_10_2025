# Build stage
FROM golang:1.25-alpine AS builder

# Enable experimental garbage collector (for Go 1.21+)
ENV GOGC=off
ENV GOMEMLIMIT=512MiB

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o pipeline-app

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/pipeline-app .
COPY --from=builder /app/pipeline/sample-event.json ./pipeline/

# Expose port for metrics (if needed)
EXPOSE 8080

# Run the application
ENTRYPOINT ["./pipeline-app"]
