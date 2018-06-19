#!/bin/bash
set -ex

export GOPATH=$(pwd)/go
export PATH=${PATH}:${GOPATH}/bin
## TODO:
#    Install GPDB binaries
#    init gpdb cluster
#    Set PG* env variables
#    source greenplum_path
#

function install_gpdb() {
    [ ! -d /usr/local/greenplum-db-devel ] && mkdir -p /usr/local/greenplum-db-devel
    if [ -f ./bin_gpdb/bin_gpdb.tar.gz ]; then
        tar -xzf bin_gpdb/bin_gpdb.tar.gz -C /usr/local/greenplum-db-devel 
    else
        # TODO
        echo "This script expects that the pipeline places the file ... at the location ... "
        echo "please modify your pipeline to make it so..."
        exit 1
    fi
}

function setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
}

function make_cluster() {
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  #export BLDWRAP_POSTGRES_CONF_ADDONS=${BLDWRAP_POSTGRES_CONF_ADDONS}
  # Currently, the max_concurrency tests in src/test/isolation2
  # require max_connections of at least 129.
  export DEFAULT_QD_MAX_CONNECT=150
  export STATEMENT_MEM=250MB
  pushd gpdb_src/gpAux/gpdemo
  su gpadmin -c "make create-demo-cluster"
  popd
}


function _main() {

install_gpdb
setup_gpadmin_user
make_cluster

chown -R gpadmin:gpadmin ${GOPATH}
su gpadmin -c 'source /usr/local/greenplum-db-devel/greenplum_path.sh
source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
export GOPATH=$(pwd)/go
export PATH=${PATH}:${GOPATH}/bin
cd ${GOPATH}/src/github.com/greenplum-db/gpupgrade
make depend
make integration'

}

_main "$@"



