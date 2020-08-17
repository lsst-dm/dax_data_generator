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
from lsst.dax.data_generator import columns
from lsst.dax.data_generator import DataGenerator
from lsst.dax.distribution.DataGenConnection import DataGenConnection
from lsst.dax.distribution.DataGenClient import DataGenClient

#TODO: add arguments for server host, port, and maybe working directory
if __name__ == "__main__":
    dgClient = DataGenClient("127.0.0.1", 13042)
    dgClient.run()

