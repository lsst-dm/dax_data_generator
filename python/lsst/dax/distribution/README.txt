
For DataGenClient.py to run dax_data_generator/bin/datagen.py, sphgeom needs
to be setup with EUPS.

serverCfg.yml contains configuration information for DataGenClient.py. Included
in this file are command line arguments for datagen.py and the name of the
configuration file (in this case fakeGenSpec.py) used when the client runs
datagen.py.

fakePartitionerCfgs is a directory containing configuration files for the
partitioner. There needs to be a configuration file for each table to be
partitioned and placed in the database. The name of the configuration
file needs to be the same as the table name. For example the
'Object' table would need 'fakePartitionerCfgs/Object.cfg' configuration
file. These configuration files are also sent to the clients by the server.

Table names should not contain the underscore character. This could negatively
impact regular expressions used to identify files by the clients.

The server waits for clients to connect. When a client connects, it passes the
client the command line arguments and the entire configuration file for
datagen.py (in this case fakeGenSpec.py). It then sends all configuration
files for the partitioner. The client then asks for some chunks numbers to
create, which the server supplies. Once the client finishes making the
chunks, it sends a message back to the server indicating which chunks were
made successfully. The client then asks for more chunks. This repeats until
the server runs out of chunks that need to be created.
When all the clients are finished, the server provides a report of the results.

If a client runs into significant difficulty, it gives up and sends the
server a list of completed(ingested) chunks (if any) and exits.

Multiple clients can run on the same host, but they must have different
working directories or there are posible conflicts with incomplete
files as well as complete vs edge only csv files.

Replace 'work' with the appropriate development directory.

Starting distribution server:
  cd ~/stack
  . loadLSST.bash
  cd ~/work/qserv
  setup -r . -t qserv-dev
  cd ../sphgeom/
  setup -k -r . -t qserv-dev
  cd ../dax_data_generator/
  python setup.py develop
  cd ./python/lsst/dax/distribution/
  python DataGenServer.py

starting distribution client:
  cd ~/stack/
  . loadLSST.bash
  cd ~/work/qserv
  setup -r . -t qserv-dev
  cd ../sphgeom/
  setup -k -r . -t qserv-dev
  cd ../partition
  setup -k -r . -t qserv-dev
  cd ../parquet_tools
  python setup.py develop
  cd ../dax_data_generator/
  python setup.py develop
  cd ./python/lsst/dax/distribution/
  python DataGenClient.py



These may be needed by the client as well
pip install healpy
pip install pyarrow
