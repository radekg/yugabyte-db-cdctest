FROM golang:1.16-alpine3.15 as builder
RUN apk add alpine-sdk ca-certificates

WORKDIR /go/src/github.com/radekg/yugabyte-db-cdctest
COPY . .

ARG MAKE_TARGET=build
ARG GOOS=linux
ARG GOARCH=amd64
RUN make -e GOARCH=${GOARCH} -e GOOS=${GOOS} clean ${MAKE_TARGET}

FROM alpine:3.15
RUN apk add --no-cache openssl ca-certificates

COPY --from=builder /go/src/github.com/radekg/yugabyte-db-cdctest/build /opt/yugabyte-db-cdctest/bin
ENTRYPOINT ["/opt/yugabyte-db-cdctest/bin/yugabyte-db-cdctest"]
CMD ["--help"]
