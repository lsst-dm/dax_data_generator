Building and using the container
================================

IMPORTANT: make sure your branch mentioned below is in GitHub! The local Git
repo will be IGNORED by the Dockerfile.

Assuming the container is build off the `master` branch of the package, do tis
from the top directory of the package:
```
BRANCH=master
cat container/Dockerfile.tmpl | sed 's/{{BRANCH}}/'${BRANCH}'/g' > container/Dockerfile
docker build -t qserv/dax_data_generator:tools -f container/Dockerfile .
docker push qserv/dax_data_generator:tools
```
or for tickets/DM-12345
```
cat container/Dockerfile.tmpl | sed  's/{{BRANCH}}/tickets\/DM-12345/g' > container/Dockerfile
docker build -t qserv/dax_data_generator:tools-DM-12345 -f container/Dockerfile .
docker push qserv/dax_data_generator:tools-DM-12345
```

To start the client:
```
docker run --network=host --rm -it -u 1000:1000 qserv/dax_data_generator:tools \
  /bin/bash -c "/home/qserv/dax_data_generator/scripts/run_datagenclient.sh"
```

*TODO*: add wrapper scripts to run other services, tests, etc. inside the container.
Look inside the above shown sample wrapper script for further details and requirements
for these scripts.
