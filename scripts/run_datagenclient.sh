#!/bin/bash
source /qserv/stack/loadLSST.bash
setup -t qserv-dev qserv_distrib
cd /home/qserv/sphgeom
setup -k -r . -t qserv-dev
cd /home/qserv/dax_data_generator/
python bin/datagenclient.py
