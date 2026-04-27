# Build stage
FROM golang:1.25-alpine AS build

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /worker ./cmd/worker

# Final stage
FROM gcr.io/distroless/static-debian12

# Copy binaries
COPY --from=build /server /server
COPY --from=build /worker /worker

# Copy migrations (optional, for init containers)
COPY --from=build /app/migrations /migrations

# Default to server
ENTRYPOINT ["/server"]