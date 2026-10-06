# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
# Static binary so the runtime image needs nothing but ca-certificates-free base.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/logfollow ./cmd/logfollow

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/logfollow /usr/local/bin/logfollow
# The log directory is bind-mounted read-only; this is just the mount point.
USER nonroot:nonroot
EXPOSE 8080
ENV LOGFOLLOW_ADDR=":8080" \
    LOGFOLLOW_DIR="/logs"
ENTRYPOINT ["/usr/local/bin/logfollow"]
