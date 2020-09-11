Distributed data generation - client and server
===============================================

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
For cases where you do not want to register or publish data with
the ingest system, start the server with 'python bin/datagenserver.py -k'

Starting distribution server:
  cd ~/stack
  . loadLSST.bash
  cd ~/work/qserv
  setup -r . -t qserv-dev
  cd ../sphgeom/
  setup -k -r . -t qserv-dev
  cd ../dax_data_generator/
  python setup.py develop
  python bin/datagenserver.py

starting distribution client:
  cd ~/stack/
  . loadLSST.bash
  cd ~/work/qserv
  setup -r . -t qserv-dev
  cd ../sphgeom/
  setup -k -r . -t qserv-dev
  cd ../partition
  setup -k -r . -t qserv-dev
  cd ../dax_data_generator/
  python setup.py develop
  python bin/datagenclient.py


These may be needed by the client as well
pip install healpy
pip install pyarrow



Configuration Files
===================
Configuration files should be found in configs/.

All configurations will need similar entries to those for 'fakedb'.
The names of the configuration files often matter. Such as configuration
files for the 'Object' table need to have 'Object' in their names like
'Object.cfg' or 'fakedb_Object.json'
'configs/fakedb' - contains configuration information for generating
    a basic database for testing distributed data generation.
'configs/fakedb/fakeGenSpec.py' - Python datagen.py configuration file
    that describes what tables and columns will be generated.
'configs/fakedb/serverCfg.yml' - Describes vital information for
    'bin/datagenserver.py'
'configs/fakedb/partitionerCfgs' - This directory contains configuration
    files for the partitioner. Files in this directory that end in '.cfg'
    will be copied from the server to the clients. The clients will
    generate data files for the tables that have configuration files in this
    directory.
    If fakeGenSpec.py defines tables "Object", "CcdVisit" and "ForcedSource"
    This directory will need  files "Object.cfg" and "ForcedSource.cfg"
    files. This will keep "CcdVisit" from being ingested into the
    final database.
'configs/fakedb/ingestCfgs' - Configuration files need by the ingest system.
    There needs to be a json file describing the database (see
    'configs/fakedb/ingestCfgs/fakedb.json') and one file per table in the
    final database (see 'configs/fakedb/ingestCfgs/fakedb_Object.json')



Fake Catalog Generator
======================


Requires sphgeom
pip install healpy
pip install pyarrow

Example usage:
```
python bin/datagen.py --chunk 3525 --visits 30 --objects 10000 example_spec.py
```


Internals
---------

The goal is to be able to generate individual chunks (spatial regions on the sky) independently.
This requires making some simplifications, since in a real survey visits will overlap different
chunks. This code creates a set of visit centers inside the chunk boundary, then for each visit
center and each object it creates a ForcedSource record if the object falls within a set radius of
the visit center. This means visits will appear to abruptly end at the edge of a chunk. Some
extra visits will be necessary to get the right number of ForceSource records.

An alternative would have been to generate the visit table in an initial phase, and then make chunks
in parallel using that.



Testing
=======
Some of the tests require significant resources that are probably not available
when automated tests would normally run or would take too much time.

'bin/connectiontest.py' - Test creating the sockets and using the client/server
    protocol with them.
'bin/edgefirsttest.py' - extensive test to ensure that edgefirst chunk data
    exists in complete chunks and there are no discrepancies. An extended
    version of the python unit test.
'bin/dataingesttest.py' - A test of being able to communicate with the
    replicator to publish a batabase and some tables to qserv.

Setup for 'bin/dataingesttest.py'. This requires some editing to work on the
local machine ('work', 'jgates', 'stack', 'CHANGEME'). Please note that
this procedure will DESTROY local databases in qserv!

~/qserv-run/bin/qserv-stop.sh
cd
rm -rf qserv-run
cd stack
. loadLSST.bash
cd
cd work/qserv
setup -r . -t qserv-dev
cd ../qserv_testdata
setup -k -r . -t qserv-dev
cd
cd work/qserv
scons -j10 install
export QSERV_INSTALL_DIR=/home/jgates/qserv-run
qserv-configure.py -a -R $QSERV_INSTALL_DIR

mkdir $QSERV_INSTALL_DIR/var/lib/ingest/
mkdir $QSERV_INSTALL_DIR/var/lib/export

$QSERV_INSTALL_DIR/bin/qserv-start.sh
mysql --protocol=tcp -uroot -pCHANGEME -e "CREATE DATABASE qservReplica"
mysql --protocol=tcp -uroot -pCHANGEME -e "CREATE USER qsreplica@localhost"
mysql --protocol=tcp -uroot -pCHANGEME -e "GRANT ALL ON qservReplica.* TO  qsreplica@localhost"
mysql --protocol=tcp -uroot -pCHANGEME -e "UPDATE qservw_worker.Id SET id='worker'"

$QSERV_INSTALL_DIR/bin/qserv-stop.sh
$QSERV_INSTALL_DIR/bin/qserv-start.sh

cat /home/jgates/work/qserv/core/modules/replica/sql/replication.sql | mysql --protocol=tcp -uroot -pCHANGEME qservReplica

cd $QSERV_INSTALL_DIR
cp ~/replicator/* .
cat ./replication.sql | mysql --protocol=tcp -uroot -pCHANGEME qservReplica

export CONFIG="mysql://qsreplica@localhost:3306/qservReplica"
export LSST_LOG_CONFIG=$PWD/log4cxx.replication.properties
qserv-replica-master-http --debug --config=$CONFIG --instance-id=mono --qserv-db-password=CHANGEME >& $QSERV_INSTALL_DIR/var/log/qserv-replica-master-http.log&
qserv-replica-worker worker --debug --config=$CONFIG --instance-id=mono --qserv-db-password=CHANGEME >& $QSERV_INSTALL_DIR/var/log/qserv-replica-worker.log&
ps -auxwww | grep "qserv-replica-"


Restarting a Problem Run with Log Files
=======================================
The chunks are written to chunk log files in the provided log directory
(default '~/log').
If there are any problems the log files can be moved to a new
directory (possibly edited) and used as input files to continue.

Simply put if there a problem is encountered with:
  bin/datagenserver.py -o "~/log/" -r "0:200000"
It can be restarted with:
  bin/datagenserver.py -i "~/log/" -o "~/new_log/"
Note that the original log directory is the input and a new output log
directory is defined. Also, it's likely the log files will need to be
edited.

The log files are:
    target.clg - all chunks that should be created
    assigned.clg - all chunks that were assigned to clients to be created
    completed.clg - all chunks that were created
    limbo.clg - chunks that clients failed to create
The limbo.clg file is likely incomplete. Anything in assigned.clg but not in
completed.clg should be checked by hand. If the chunk has been created and is
complete, it should be added to completed.clg and removed from limbo.clg.
If the chunk doesn't exist or is incomplete, it should be removed from the
database, assigned.clg, limbo.clg, and completed.clg so that it will be
generated on the next run.