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

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == '__main__':

    # &&& 1000 visits per night, 0.5 years of observing.
    # &&& n_visits = 1000*150
    # 1000 visits/night for 1 year, full sky
    n_visits = 1000*300
    area = 20000
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
    ord = np.sort(dec)
    plt.plot(ord)
    plt.show()

    df = pd.DataFrame({"visitId": visit_id, "filter": filter_name, "ra": ra, "decl": dec})
    df.to_csv("visit_table.csv", index=False, header=False)


