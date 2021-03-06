#!/usr/bin/env python3

# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import getopt
import sys


from lsst.dax.distribution.DataGenClient import DataGenClient


def usage():
    print("-h, --help  help")
    print("-H, --host  server host IP adress.")
    print("-P, --port  server port number")
    print("-r, --retry retry connecting to server")
    print("-C, --chunks chunks per request to server (default 5)")


if __name__ == "__main__":
    host = "127.0.0.1"
    port = 13042

    argument_list = sys.argv[1:]
    print("argumentList=", argument_list)
    options = "hH:C:P:r"
    long_options = ["help", "host", "port", "retry"]
    skip_ingest = False
    skip_schema = False
    retry = False
    chunks_per_req = 1
    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                exit(0)
            elif arg in ("-H", "--host"):
                host = val
            elif arg in ("-P", "--port"):
                port = int(val)
            elif arg in ("-r", "--retry"):
                retry = True
            elif arg in ("-C", "--chunks"):
                chunks_per_req = val
    except getopt.error as err:
        print(str(err))
        exit(1)
    print(f'server {host}:{port}')
    dg_client = DataGenClient(host, port, retry=retry, chunks_per_req=chunks_per_req)
    dg_client.run()

