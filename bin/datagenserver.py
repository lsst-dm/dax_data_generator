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

import lsst.dax.distribution.chunklistfile as chunklistfile
from lsst.dax.distribution.DataGenServer import DataGenServer

def usage():
    print('-h, --help  help')
    print('-k, --skipIngest  skip trying to ingest anything')
    print('-s, --skipSchema  skip sending schema, needed when schema was already sent.')
    print('-o, --outDir      output log directory default "~/log/"')
    print('-i, --inDir       input directory, only "target.out" must exist\n'
          '                  ex: "~/log/" which would look for\n'
          '                      ~/log/target.out, ~/log/completed.out,\n'
          '                      ~/log/assigned.out, and ~/log/imbo.out')
    print('-r, --raw  string describing targets chunk ids such as "0:10000" or "0,1,3,466"')
    print('')
    print('If niether -i or -r are specified, target list will include all valid chunks ids.')
    print('If -r and -i are both specified, target list will be union of target file and\n'
          '-r option while completed, assigned, and limbo lists created from the files found.')

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
    options = "hksc:i:"
    long_options = ["help", "skipIngest", "skipSchema", "configfile"]
    skip_ingest = False
    skip_schema = False
    config_file = "configs/fakedb/serverCfg.yml"
    in_dir = None
    out_dir = "~/log/"
    raw = None
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                return False
            elif arg in ("-k", "--skipIngest"):
                skip_ingest = True
            elif arg in ("-s", "--skipSchema"):
                skip_schema = True
            elif arg in ("-c", "--configfile"):
                config_file = val
            elif arg in ("-o", "--outDir"):
                out_dir = val
            elif arg in ("-i", "--inDir"):
                in_dir = val
            elif arg in ("-r", "--raw"):
                raw = val
    except getopt.error as err:
        print (str(err))
        exit(1)
    print("skip_ingest=", skip_ingest, "skip_schema=", skip_schema, "values=", values)
    print(f"configfile={config_file} in_dir={in_dir} raw={raw}")
    # If in_dir is defined (empty string is valid), see if files can be found
    if not in_dir is None:
        # Throws if targetf not found
        targetf, completedf, assignedf, limbof = chunklistfile.ChunkLogs.checkFiles(in_dir)
        print(f"target={targetf} completed={completedf} assigned={assignedf} limbo={limbof}")
        clfs = chunklistfile.ChunkLogs(targetf, completedf, assignedf, limbof)
    else:
        clfs = chunklistfile.ChunkLogs(None)
    #&&& chunklistfile.ChunkLogs
    #&&&clfs = chunklistfile.ChunkLogs(dummyf)
    # 0-50000 would be all chunks for stripes = 200 substripes = 5
    #&&&dgServ = DataGenServer(config_file, 0, 2000, skip_ingest, skip_schema)
    dgServ = DataGenServer(config_file, clfs, out_dir, skip_ingest, skip_schema)
    dgServ.start()

if __name__ == "__main__":
    server()

