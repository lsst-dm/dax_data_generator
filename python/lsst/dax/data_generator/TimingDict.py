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


class TimingDict
    """A dictionary used to store timing information

    Parameters
    ----------
    timing_dict : dictionary
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

    def __init__(self, timing_dict):
        self.timing_dict = timing_dict
        if self.timing_dict is None:
            self.reset()

    def __repr__(self):
        return str(self.timing_dict.items())

    def reset(self):
        """Clear the dictionary
        """
        self.timing_dict = {}

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
        if key in self.timing_dict:
            self.timing_dict[key] += float(val)
        else:
            self.timing_dict[key] = float(val)

    def combine(self, other):
        """Merge another timing_dict object with this one.

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
        for key, val in other:
            if key in self.timing_dict:
                self.timing_dict[key] += val
            else:
                self.timing_dict[key] = val

    def report(self):
        """Get a somewhat well formed string of the contents of this object.
        """
        st = "Times\n"
        COUNT = 'count' # reserved string
        count = None
        width = 5
        if COUNT in self.timing_dict:
            if self.timing_dict[COUNT] != 0:
                count = self.timing_dict[COUNT]
        sum = 0.0
        for key, val in self.timing_dict:
            if key == COUNT:
                continue
            if (len(key) > width):
                width = len(key)
            sum += val
        if count is not None:
            st += f'count={count} with a total time of {sum} and avg of {sum/count}\n'
        else:
            st += 'count 0 or unavailable\n'
        for key, val in self.timing_dict:
            st += f'{key:<{width}}={val:9.3f}'
            if count is not None:
                st += f' avg={val/count:9.3f} {val*100.0/sum:3.1f}%\n'
        return st


