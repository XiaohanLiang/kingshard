#!/bin/zsh

function build() {
  GO111MODULE=on \
  GOPROXY='https://goproxy.cn' \
  go build -o artifact/kingshard \
  main.go
}

build