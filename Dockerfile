FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/logfollower ./cmd/logfollower

FROM alpine:3.20
RUN adduser -D -H -u 10001 follower
USER 10001
COPY --from=build /out/logfollower /usr/local/bin/logfollower
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/logfollower"]
