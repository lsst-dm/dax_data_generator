#!/usr/bin/env python

# This file is part of dax_data_generator.
#
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
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pathlib
import sys


def usage():
    print('-h, --help  help')
    print('-o, --outFile     Output file name')
    print('-p, --plot        plot declinations')
    print('-n, --numOfVisits Number of visits')


def create_visits():
    """Start the server.
    """
    argumentList = sys.argv[1:]
    print("argumentList=", argumentList)
    options = "ho:pn:"
    long_options = ["help", "outFile", "plot", "numOfVisits"]
    out_file = "visit_table.csv"
    plot = False
    n_visits = 1000*150  # 1000 visits/night for 0.5 years
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                return False
            elif arg in ("-o", "--outFile"):
                out_file = val
            elif arg in ("-p", "--plot"):
                plot = True
            elif arg in ("-n", "--numOfVisits"):
                n_visits = val
    except getopt.error as err:
        print(str(err))
        exit(1)
    print(f"outFile={out_file} numOfVisits={n_visits}\n")

    # area = 20000
    north_only = False

    visit_id = np.arange(n_visits)
    filter_name = np.random.choice(["u", "g", "r", "i", "z", "y"], n_visits)

    ra = 360*np.random.rand(n_visits)

    if north_only:
        cos_dec = np.random.rand(n_visits)
    else:
        cos_dec = 2*np.random.rand(n_visits) - 1
    dec = np.degrees(np.arccos(cos_dec)) - 90.0
    print("dec", dec)
    if plot:
        ord = np.sort(dec)
        plt.plot(ord)
        plt.show()

    df = pd.DataFrame({"visitId": visit_id, "filter": filter_name, "ra": ra, "decl": dec})
    df.sort_values(by=['decl'])

    out_dir = pathlib.Path(out_file).parent
    print("making directories ", out_dir)
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False, header=False)


if __name__ == '__main__':
    create_visits()

