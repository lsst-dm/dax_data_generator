Building and using the container
================================

IMPORTANT: make sure your branch mentioned below is in GitHub! The local Git
repo will be IGNORED by the Dockerfile.

IMPORTANT: configurations are complicated, see note at the end if changing
anything.


Client
------
Many clients can (and should) be run simultaneously. The only configuration
clients need is how to connect to the server. (see the last example for
setting host and port number when starting a client)

Assuming the container is build off the `master` branch of the package, do tis
from the top directory of the package:
```
container/build container master
```

To build a branch for a ticket. The ticket branch MUST BE IN GITHUB as
any local changes to the branch are ignored.
```
container/build container tickets/DM-26409 --no-cache
```
'--no-cache' is useful when the code in github changes


To start the client, master branch:
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:master \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenclient.sh"
```

To start the client branch tickets-DM-26409
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:tickets-DM-26409 \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenclient.sh"
```

To start the client master branch to connect to the server on a different host with
a different port. (See dax_data_generator/bin/datagenclient.py for more client
command line arguments.)
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:master \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenclient.sh -H some.comp.edu -p 12345"
```


Server
------
The server is found inside the same container as the client, so building
the client builds the server.

Basic starting the server
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:tickets-DM-26409 \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenserver.sh"
```

Starting the server without sending anything to the ingest system. Useful for testing
data generation configuration files and code.
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:tickets-DM-26409 \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenserver.sh -k"
```

Starting the server without sending table schema information to the ingest system.
Useful when the schema information has already been sent. Using the fakedb configuration
file
```
docker run --network=host --rm -u 1000:1000 qserv/dax_data_generator:tickets-DM-26409 \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenserver.sh -s \
  -c configs/fakedb/serverCfg.yml"
```

Configurations can also be added by using an external volume for the configuration
directories.
```
docker run --network=host --rm -u 1000:1000 \
  -v fakeGenSpec.py:/home/qserv/dax_data_generator/config/fakedb/fakeGenSpec.py:ro \
  qserv/dax_data_generator:tickets-DM-26409 \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenserver.sh
```


NOTE - Configuration files are complicated and tightly bound.
- TEST CHANGES AND NEW CONFIGS ON A SMALL LOCAL SYSTEM WITH A SMALL SUBSET OF CHUNKS.
- CHANGING A CONFIGURATION IS ALMOST GUARANTEED TO CHANGE THE DATA AND INVALIDATE
  THE EXISITNG DATA (i.e. you'll need to rebuild the entire dataset for most config changes
  the most notable exception being the set of chunks generated).

The distributed data generation ties together a few other software systems, so
values are repeated in different configs. Generating all the configuration files
from a single file would be nice, but it will require effort that is probably
better spent eslewhere.

Configuration files for generating a particular dataset should be added to the git
repository in their own config subdirectory. 'dax_data_generator/config/fakedb'
contains all the configuration information for 'fakedb' which is used for basic
testing. There will be 'dax_data_generator/config/kpm50a' for kpm50 tests.

It will probably be desirable to hold on to some configurations for regression
testing.

*TODO*: add wrapper scripts to run other services, tests, etc. inside the container.
Look inside the above shown sample wrapper script for further details and requirements
for these scripts.
