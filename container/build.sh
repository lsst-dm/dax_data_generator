#!/bin/bash

set -e

CONTAINER_TOOLS_DIR="$1"
USAGE="
Usage:
    build.sh ${CONTAINER_TOOLS_DIR} <branch> [--no-cache]
    ex: ./build . tickets/DM-26409 --no-cache

Where:
  <branch> - the name of the Git branch to build the container from. Note that if you
             have a local copy of the branch then commit and push your changes to
             GitHub before building the container.
             The branch name will also be used to create a custom tag for the
             container. For example, branch 'tickets/DM-26409' will map to
             tag 'qserv/dax_data_generator:tickets-DM-26409'. And branch 'master' will
             make tag 'qserv/dax_data_generator:master'.
  [--no-cache] - Optional argument to the docker build command.
"

BRANCH="$2"
if [ -z "${BRANCH}" ]; then
    >&2 echo $USAGE
    exit 1
fi
TAG="qserv/dax_data_generator:$(echo $BRANCH | tr '/' '-')"
cd $CONTAINER_TOOLS_DIR
cat Dockerfile.tmpl | sed 's\{{BRANCH}}\'${BRANCH}'\g' > Dockerfile
docker build $3 -t $TAG -f Dockerfile .
docker push $TAG
rm Dockerfile
