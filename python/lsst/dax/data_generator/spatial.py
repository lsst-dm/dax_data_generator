# This file is part of dax_data_generator.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import abc
import healpy

__all__ = ["SpatialModel", "UniformSpatialModel", "HealpixSpatialModel"]

class SpatialModel(abc.ABC):

    @abc.abstractmethod
    def get_density_at_point(self, ra, dec):
        """Must return Objects per square degree"""
        pass


class UniformSpatialModel(SpatialModel):

    def __init__(self, density):

        self.density = density

    def get_density_at_point(self, ra, dec):
        return self.density

class HealpixSpatialModel(SpatialModel):

    def __init__(self, healpix_density_map, nside, nested):
        self.nside = nside
        self.nested = nested
        self.healpix_density_map = healpix_density_map

    def get_density_at_point(self, ra, dec):
        healpix_id = healpy.ang2pix(self.nside, ra, dec, nest=self.nested, lonlat=True)
        density = self.healpix_density_map[healpix_id]

        return density

