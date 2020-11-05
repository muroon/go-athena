FROM golang:1.14

WORKDIR /go/src/github.com/speee/go-athena

ENV GO111MODULE=on

COPY . .
