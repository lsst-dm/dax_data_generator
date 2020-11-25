#!/bin/bash
source /qserv/stack/loadLSST.bash
setup -t qserv-dev qserv_distrib
cd /home/qserv/sphgeom
setup -k -r . -t qserv-dev
cd /home/qserv/partition
setup -k -r . -t qserv-dev
cd /home/qserv/dax_data_generator/
echo starting
python bin/datagenclient.py "$@" > /home/qserv/logfile
echo finished
