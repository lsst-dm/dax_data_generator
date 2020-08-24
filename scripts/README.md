Launching scripts via the container
===================================

Assuming you have Docker privileges on deployment machines and the right tag
for the container get this container like shown below:
```
docker pull qserv/dax_data_generator:tools
```

To start the client (replace 'tools' with 'tools-DM-<branch#>'):
```
docker run --network=host --rm -it -u 1000:1000 qserv/dax_data_generator:tools \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenclient.sh"
```

*TODO*: add wrapper scripts to run other services, tests, etc. inside the container.
Look inside the above shown sample wrapper script for further details and requirements
for these scripts.

