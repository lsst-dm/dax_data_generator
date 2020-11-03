#! /bin/bash

set -e

# This needs to br run from dax_data_generator/ directory.
python bin/makevisits.py -o localConfig/pregenerated/visit_table.csv
