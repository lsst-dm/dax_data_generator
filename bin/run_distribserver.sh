#! /bin/bash

set -e

# Use: (This is expected to be called from the dax_data_generator directory)
#   cd dax_data_generator
#   bin/run_distribserver.sh <database_name> -g<replicator_server_host> <other options>
#
# ex: bin/run_distribserver.sh  fakedb -glocalhost -k -r "0:2000"

# Copy the configuration directory for the specified database to localConfig
# after deleteing localConfig.
rm -rf localConfig
cp -r configs/$1 localConfig

# Do any specific building for the database
/bin/bash localConfig/prepare.sh
ls -la localConfig

# start the server
python bin/datagenserver.py ${@: 2}
