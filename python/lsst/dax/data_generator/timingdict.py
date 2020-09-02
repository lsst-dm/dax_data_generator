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

import time


class TimingDict:
    """A dictionary used to store timing information

    Parameters
    ----------
    times : dictionary
        A dicitionary with str keys and float values. The dictionary is
        used internally by the class. If it is None, an empty dictionary
        is created.

    Notes
    -----
    While this is intended to store timing information, the only hard
    requirement is that all the keys are strings and all the values are
    floats.
    The key "count" is reserved for the total count.
    """

    def __init__(self):
        self.times = {}
        self.count = 0

    def __repr__(self):
        return f'TimingDict count={self.count} {self.times.items()}'

    def __eq__(self, other):
        return self.count == other.count and self.times == other.times

    def add(self, key, val):
        """Add val to the corresponding key.

        Parameters
        ----------
        key : str
            The key name.
        val : float
            Value to add to the existing value. Existing value is 0 for
            undefined keys.
        """
        if key in self.times:
            self.times[key] += float(val)
        else:
            self.times[key] = float(val)

    def start(self):
        """Return a float start_time.
        """
        return time.time()

    def end(self, key, start_time):
        """Add a timing entry for 'key'

        Parameters
        ----------
        key : str
            Key the measurment should be added to.
        start_time : float
            Starting time for a measurent.
        """
        end_time = time.time()
        val = end_time - start_time
        self.add(key, val)

    def increment(self):
        """Increment "count"
        """
        self.count += 1

    def combine(self, other):
        """Merge another TimingDict object with this one.

        Parameters
        ----------
        other : TimingDict
            Another TimingDict object

        Notes
        -----
        The 'other' object will be merged with this one. In cases
        where the keys match, the 'other' values will be added to
        the corresponding values in this object. In cases where
        the 'other' key does not exist, it will be created with
        the 'other' value.
        """
        for key, val in other.times.items():
            if key in self.times:
                self.times[key] += val
            else:
                self.times[key] = val
        self.count += other.count

    def report(self):
        """Get a somewhat well formed string of the contents of this object.
        """
        st = "Times\n"
        width = 5 if not self.times else max(len(x) for x in self.times.keys())
        sum = 0.0
        for key, val in self.times.items():
            sum += val
        st += f'count={self.count} with a total time of {sum}'
        if sum != 0.0:
            st += f' and avg of {sum/self.count}'
        st += '\n'
        for key, val in self.times.items():
            st += f'{key:<{width}}={val:9.3f}'
            if self.count != 0:
                st += f'   avg={val/self.count:9.3f}'
            if sum != 0.0:
                st += f' {val*100.0/sum:3.1f}%'
            st += '\n'
        return st

