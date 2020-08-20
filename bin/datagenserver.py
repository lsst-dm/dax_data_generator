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

import argparse
import getopt
import sys

from lsst.dax.distribution.DataGenServer import DataGenServer


def server():
    """Temporary main function call.
    TODO: The next step is to have the chunks to be created read in from files
    and have this program produce a files of that format for created chunks
    and chunks that should be created. That way, after the server runs
    the output from the server can be examined and the server can be run
    again with the output files to fill in the gaps.
    """
    argumentList = sys.argv[1:]
    print("argumentList=", argumentList)
    options = "hksi:"
    long_options = ["help", "skipIngest", "skipSchema", "ingestCfg"]
    skip_ingest = False
    skip_schema = False
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                print("-h, --help  help")
                print("-k, --skipIngest  skip trying to ingest anything")
                print("-s, --skipSchema  skip sending schema, needed when schema was already sent.")
                return False
            elif arg in ("-k", "--skipIngest"):
                skip_ingest = True
            elif arg in ("-s", "--skipSchema"):
                skip_schema = True
    except getopt.error as err:
        print (str(err))
        exit(1)
    print("skip_ingest=", skip_ingest, "skip_schema=", skip_schema, "values=", values)
    # 0-50000 would be all chunks for stripes = 200 substripes = 5
    dgServ = DataGenServer("configs/fakedb/serverCfg.yml", 0, 2000, skip_ingest, skip_schema)
    dgServ.start()

if __name__ == "__main__":
    server()

