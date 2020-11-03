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

from pathlib import Path
import getopt
import sys

import lsst.dax.distribution.chunklogs as chunklogs
from lsst.dax.distribution.DataGenServer import DataGenServer


def usage():
    print('-h, --help  help')
    print('-c, --configfile  Configuration file name. The file must be in \n'
          '                  dax_data_generator/localConfig')
    print('-g, --ingestHost  IP address of the ingest host (replicator server)')
    print('-k, --skipIngest  Skip trying to ingest anything')
    print('-s, --skipSchema  Skip sending schema, needed when schema was already sent.')
    print('-o, --outDir      Output log directory defaults to current directory')
    print('-i, --inDir       Input directory, only "target.clg" must exist\n'
          '                  ex: "~/in/" which would look for\n'
          '                      ~/in/target.clg, ~/in/completed.clg,\n'
          '                      ~/in/assigned.clg, and ~/in/imbo.clg')
    print('-r, --raw         String describing targets chunk ids such as "0:10000"\n'
          '                  or "0,1,3,466"')
    print('-z, --keepCsv     Hold onto intermediate csv files for debugging.')
    print('')
    print('If niether -i or -r are specified, target list will include all valid chunks ids.')
    print('If -r and -i are both specified, target list will be union of target file and\n'
          '-r option while completed, assigned, and limbo lists are created from the files\n'
          'found.')
    print('test ex: bin/datagenserver.py -k -z -o "~/log/" -r "0:2000"')
    print('\nSee README.md "Restarting a Problem Run with Log Files" for information')
    print('on using log files to continue a previous run with problems.')


def server():
    """Start the server.
    """
    argumentList = sys.argv[1:]
    print("argumentList=", argumentList)
    options = "hksc:g:i:o:r:z"
    long_options = ["help", "skipIngest", "skipSchema", "configfile", "outDir", "inDir", "raw", "keepCsv"]
    skip_ingest = False
    skip_schema = False
    config_file = "serverCfg.yml"
    ingest_host = "127.0.0.1"
    in_dir = None
    out_dir = ""
    raw = None
    keep_csv = False
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
            elif arg in ("-g", "--ingestHost"):
                ingest_host = val
            elif arg in ("-o", "--outDir"):
                out_dir = val
            elif arg in ("-i", "--inDir"):
                in_dir = val
            elif arg in ("-r", "--raw"):
                raw = val
            elif arg in ("-z", "--keepCsv"):
                keep_csv = True
    except getopt.error as err:
        print(str(err))
        exit(1)
    print("skip_ingest=", skip_ingest, "skip_schema=", skip_schema, "values=", values)
    print(f"configfile={config_file} in_dir={in_dir} raw={raw}\n")

    # Check that configFile exists and make it the absolute path
    abs_path_cwd = Path.cwd()
    config_file_path = abs_path_cwd / "localConfig" / config_file
    if not config_file_path.is_file():
        print(f"ERROR: config_file {config_file} -> {config_file_path} is not a file, exiting")
        exit(1)

    print("config_file_path", config_file_path)
    # Replace #INGEST_HOST# with ingest_host in the file
    with open(config_file_path, 'r') as cfg_file:
        cfg_contents_in = cfg_file.read()

    with open(config_file_path, 'w') as cfg_file:
        cfg_file.write(cfg_contents_in.replace('#INGEST_HOST#', ingest_host))

    # If in_dir is defined (empty string is valid), see if files can be found
    if in_dir is not None:
        # Throws if targetf not found
        targetf, completedf, assignedf, limbof = chunklogs.ChunkLogs.checkFiles(in_dir)
        print(f"target={targetf} completed={completedf} assigned={assignedf} limbo={limbof}")
        clfs = chunklogs.ChunkLogs(targetf, completedf, assignedf, limbof, raw)
    else:
        clfs = chunklogs.ChunkLogs(None, raw=raw)
    # 0-50000 would be all chunks for stripes = 200 substripes = 5
    dgServ = DataGenServer(config_file_path, clfs, out_dir, skip_ingest, skip_schema, keep_csv)
    if dgServ.chunksToSendTotal() == 0:
        print("No chunks to generate, exiting.")
        exit(0)
    dgServ.start()


if __name__ == "__main__":
    server()

