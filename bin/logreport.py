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

import lsst.dax.distribution.chunklogs as chunklogs

def usage():
    print('-h, --help  help')
    print('-k, --skipIngest  skip trying to ingest anything')
    print('-s, --skipSchema  skip sending schema, needed when schema was already sent.')
    print('-o, --outDir      output log directory default "~/log/"')
    print('-i, --inDir       input directory, only "target.clg" must exist\n'
          '                  ex: "~/in/" which would look for\n'
          '                      ~/in/target.clg, ~/in/completed.clg,\n'
          '                      ~/in/assigned.clg, and ~/in/imbo.clg')
    print('-r, --raw  string describing targets chunk ids such as "0:10000" or "0,1,3,466"')
    print('')
    print('If niether -i or -r are specified, target list will include all valid chunks ids.')
    print('If -r and -i are both specified, target list will be union of target file and\n'
          '-r option while completed, assigned, and limbo lists created from the files found.')
    print('test ex: bin/datagenserver.py -k -o "~/log/" -r "0:2000"')
    print('\nSee README.md "Restarting a Problem Run with Log Files" for information')
    print('on using log files to continue a previous run with problems.')

def server():
    """Start the server.
    """
    argumentList = sys.argv[1:]
    print("argumentList=", argumentList)
    options = "hi:"
    long_options = ["help", "inDir"]
    in_dir = "~/log/"
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                return False
            elif arg in ("-i", "--inDir"):
                in_dir = val
    except getopt.error as err:
        print (str(err))
        exit(1)
    print(f"in_dir={in_dir}\n")
    # If in_dir is defined (empty string is valid), see if files can be found
    # Throws if targetf not found
    targetf, completedf, assignedf, limbof = chunklogs.ChunkLogs.checkFiles(in_dir)
    print(f"target={targetf}\ncompleted={completedf}\nassigned={assignedf}\nlimbo={limbof}\n")
    clogs = chunklogs.ChunkLogs(targetf, completedf, assignedf, limbof, None)
    clogs.build(None)
    print(clogs.report())

if __name__ == "__main__":
    server()

