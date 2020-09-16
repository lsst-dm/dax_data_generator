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

import lsst.dax.distribution.chunklogs as chunklogs


def usage():
    print()
    print('Analyze the log files and produce a list of chunk_ids that had problems.')
    print('Essentially, any chunk_id found in assigned.clg but not in completed.clg')
    print('needs to be checked.')
    print('-h, --help  help')
    print('-i, --inDir       input directory, only "target.clg" must exist\n'
          '                  ex: "~/log/" which would look for\n'
          '                      ~/log/target.clg, ~/log/completed.clg,\n'
          '                      ~/log/assigned.clg, and ~/log/imbo.clg')
    print('\nSee README.md "Restarting a Problem Run with Log Files" for information')
    print('on using log files to continue a previous run that had problems.')


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    print("argumentList=", argument_list)
    options = "hi:"
    long_options = ["help", "inDir"]
    in_dir = "~/log/"
    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                exit(1)
            elif arg in ("-i", "--inDir"):
                in_dir = val
    except getopt.error as err:
        print(str(err))
        print(usage())
        exit(1)
    print(f"in_dir={in_dir}\n")
    # If in_dir is defined (empty string is valid), see if files can be found
    # Throws if targetf not found
    targetf, completedf, assignedf, limbof = chunklogs.ChunkLogs.checkFiles(in_dir)
    print(f"target={targetf}\ncompleted={completedf}\nassigned={assignedf}\nlimbo={limbof}\n")
    clogs = chunklogs.ChunkLogs(targetf, completedf, assignedf, limbof, None)
    clogs.build(None)
    print(clogs.report())

