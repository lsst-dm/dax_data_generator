#!/bin/bash
source /qserv/stack/loadLSST.bash
setup -t qserv-dev qserv_distrib
cd /home/qserv/dax_data_generator/
# Folowing needs to be generalized
python bin/makevisits.py -o configs/fakedb/pregenerated/visit_table.csv
python bin/datagenserver.py "$@"
