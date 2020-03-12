
For DataGenClient.py to run dax_data_generator/bin/datagen.py, sphgeom needs to be setup with EUPS.

serverCfg.yml contains configuration information for DataGenClient.py. Included in this file are command line arguments for datagen.py and the name of the configuration file (in this case fakeGenSpec.py) used when the client runs datagen.py.

The server waits for clients to connect. When a client connects, it passes the client the command line arguments and the entire configuration file for datagen.py (in this case fakeGenSpec.py). The client then asks for some chunks numbers to create, which the server supplies. Once the client finishes making the chunks, it sends a message back to the server indicating which chunks were made successfully. The client then asks for more chunks. This repeats until the server runs out of chunks that need to be created. When all the clients are finished, the server provides a report of the results. 

The client needs the following setup to run dax_data_generator/bin/datagen.py
cd stack
. loadLSST.bash
cd <development directory containing directories for sphgeom(branch u/ctslater/getchunk), dax_data_generator>
cd sphgeom
setup -r . 
cd ../dax_data_generator
pip install .


These may be needed by the client as well
pip install healpy
pip install pyarrow
