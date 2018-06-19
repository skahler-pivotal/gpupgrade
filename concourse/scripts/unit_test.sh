#!/bin/sh
set -ex

export GOPATH=$(pwd)/go
export PATH=${PATH}:${GOPATH}/bin

cd ${GOPATH}/src/github.com/greenplum-db/gpupgrade

make depend
make unit






