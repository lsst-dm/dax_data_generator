#!/bin/bash
source /qserv/stack/loadLSST.bash
setup -t qserv-dev qserv_distrib
cd /home/qserv/dax_data_generator/

bin/run_distribserver.sh $@
