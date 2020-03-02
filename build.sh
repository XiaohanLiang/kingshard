#!/bin/zsh

function build() {
  GO111MODULE=on \
  GOPROXY='https://goproxy.cn' \
  go build -o ./kingshard \
  main.go
}

build

function updateYacc() {
    SQL_Y_PATH='./sqlparser'
    which goyacc
    if [[ $? != 0 ]]
    then
      echo 'installing goyacc.. :)'
      GOPROXY='https://goproxy.cn' go get -u golang.org/x/tools/cmd/goyacc
    fi
    cd ${SQL_Y_PATH}
    goyacc -o ./sql.go ./sql.y
    cd -
}