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


import math
import numpy as np
import os
import pandas as pd
from astropy.coordinates import SkyCoord

from .timingdict import TimingDict
from .columns import SimpleBox

__all__ = ["DataGenerator"]


class TableColumnInfo:
    def __init__(self, col_names, generator, position):
        self.col_names = col_names
        self.position = position
        self.generator = generator
        self.block = None

    def __repr__(self):
        return ("{col_names:" + self.col_names + ' position:' + str(self.position)
                + " generator:" + str(self.generator) + ' block:' + str(self.block))


class DataGenerator:
    """Create a DataGenerator based on a specification dictionary.
    The specification dictionary describes all of the tables and their
    constituent columns to generate. Nothing is generated at
    initialization; calls must be made to DataGenerator.make_chunk() to
    synthesize data.

    No validation is performed on the generator specification.
    """

    def __init__(self, spec, chunker, seed=1, pregen_dir=None):

        self.spec = spec
        self.tables = spec.keys()
        self.timingdict = TimingDict()
        self.chunker = chunker
        self.seed = seed
        self.pregen_dir = pregen_dir

    @staticmethod
    def _resolve_table_order(spec):
        """Determine the order in which tables must be built to satisfy the
        prerequisite requirements from a generator specification.

        Tables in the generator spec can contain prereq_tables and
        prereq_rows, and thus the referenced tables must be built before they
        can be used for the deriviative tables.

        There is no guarantee against circular, and thus impossible to
        construct, references.
        """
        all_tables = list(spec.keys())
        prereqs = []
        for table_name, table_spec in spec.items():
            if(table_spec.get("prereq_row", "") in all_tables):
                prereqs.append(table_spec["prereq_row"])
            for prereq_table in table_spec.get("prereq_tables", []):
                if prereq_table in all_tables:
                    prereqs.append(prereq_table)
        non_prereqs = set(all_tables) - set(prereqs)
        return prereqs + list(non_prereqs)

    def _add_to_list(self, generated_data, output_columns, split_column_names):
        """
        Takes either a single array, or a tuple of arrays each containing a
        different column of data, and appends the contents to the lists in
        the output_columns dictionary.

        The structure is output_columns[column_name] = list(np.array([]))

        Parameters
        ----------
        generated_data : tuple, list, or np.array().
            If a tuple or list is supplied, each entry is interpreted as a
            separate column identified by split_column_names.
        output_columns : dictionary
            Dictionary where the keys are column names, and the entries are
            lists of arrays containing the column data.
        split_column_names : list
            List of column names for the separate tuple elements of
            generated_data.
        """
        if isinstance(generated_data, tuple) or isinstance(generated_data, list):
            for i, name in enumerate(split_column_names):
                output_columns[name].append(generated_data[i])
        else:
            output_columns[split_column_names[0]].append(generated_data)
            if(len(split_column_names) > 1):
                raise ValueError("Column name implies multiple returns, "
                                 "but generator only returned one")

    def make_chunk(self, chunk_id, edge_width=0.017, edge_only=False, return_pregenerated=False):
        """Generate synthetic data for one chunk.

        Parameters
        ----------
        chunk_id : int
            ID of the chunk to generate.
        edge_width : float
            Width/height of the edge in degrees
        edge_only : bool
            When True, only generate objects within edge_width of the edge
            of the chunk. When False, create all objects in the chunk.
        return_pregenerated : bool
            Include tables that are loaded from a file in the output
            dictionary. This is normally only used for testing.

        Returns
        -------
        dictionary of pandas.DataFrames
            The output dictionary contains each generated table as a
            pandas.DataFrame.

        """
        logALot = False   #&&&
        if chunk_id == 1622 or chunk_id == 1623: #&&&
            logALot = True
        if logALot:
            print(f"&&& chunk_id={chunk_id} edge_width={edge_width}, edge_only={edge_only}, return_pregenerated={return_pregenerated}")

        output_tables = {}

        resolved_order = self._resolve_table_order(self.spec)
        tables_loaded_from_file = []
        if logALot:
            print(f"&&& chunk_id={chunk_id}  resolved_order={resolved_order}")

        for table in resolved_order:
            st_time = self.timingdict.start()
            if("from_file" in self.spec[table]):
                ffname = self.spec[table]["from_file"]
                if self.pregen_dir:
                    ffname = os.path.join(self.pregen_dir, ffname)
                output_tables[table] = pd.read_csv(ffname, header=None,
                                                   names=self.spec[table]["columns"].split(","))
                tables_loaded_from_file.append(table)
                continue

            column_generators = self.spec[table]["columns"]
            prereq_rows = self.spec[table].get("prereq_row", None)

            if("density" not in self.spec[table]):
                table_row_count = None
            else:
                density_model = self.spec[table]["density"]
                chunk_latlon = self.chunker.getChunkBounds(chunk_id).getCenter()
                ra_center = chunk_latlon.getLon().asDegrees()
                dec_center = chunk_latlon.getLat().asDegrees()
                chunk_density = density_model.get_density_at_point(ra_center, dec_center)
            if logALot:
                print(f"&&& chunk_id={chunk_id} ra_center={ra_center} dec_center={dec_center} chunk_density={chunk_density}")

            generated_data_per_box = []
            boxes = self._make_boxes(chunk_id, edge_width=edge_width,
                                     edge_only=edge_only)
            if logALot:
                print(f"&&& chunk_id={chunk_id} boxes={boxes}")

            for box_n, box in enumerate(boxes):
                assert(box.area() > 0)
                box_rowcount = int(chunk_density * box.area())
                if logALot:
                    print(f"&&& chunk_id={chunk_id} box_rowcount={box_rowcount} box_n={box_n} box={box}")
                if(box_rowcount == 0):
                    continue
                unique_box_id = chunk_id*8 + box_n
                box_center_ra = (box.raA + box.raB)/2.0
                box_center_dec = (box.decA + box.decB)/2.0
                box_center = SkyCoord(box_center_ra, box_center_dec, frame="icrs", unit="deg")
                output = self._generate_table_block(box, column_generators,
                                                    box_center=box_center,
                                                    row_count=box_rowcount,
                                                    prereq_rows=prereq_rows,
                                                    prereq_tables=output_tables,
                                                    unique_box_id=unique_box_id,
                                                    edge_only=edge_only)
                for name in output.keys():
                    temp = np.concatenate(output[name])
                    output[name] = temp
                generated_data_per_box.append(pd.DataFrame(output))

            output_tables[table] = pd.concat(generated_data_per_box)
            self.timingdict.end(f"gen_{table}", st_time)

        # Unless the user asks for it, we don't want to write out tables that
        # were pre-generated and loaded from a file.
        if not return_pregenerated:
            for table in tables_loaded_from_file:
                del output_tables[table]

        return output_tables

    def _make_boxes(self, chunk_id, edge_width=0, edge_only=False):

        # sphgeom Box from Chunker::getChunkBoundingBox
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        # Need to correct for RA that crosses 0.
        raA = chunk_box.getLon().getA().asDegrees()
        raB = chunk_box.getLon().getB().asDegrees()
        ra_delta = raB - raA
        if ra_delta < 0:
            raA = raA - 360.0
            ra_delta = raB - raA
        decA = chunk_box.getLat().getA().asDegrees()
        decB = chunk_box.getLat().getB().asDegrees()

        print("chunk=", chunk_id, "bbox=", chunk_box.__repr__())

        # Correct the edge_width for declination so there is at least
        # edge_width at both the top and bottom of the east and west blocks.
        edge_raA = edge_width / abs(math.cos(math.radians(decA + edge_width)))
        edge_raB = edge_width / abs(math.cos(math.radians(decB - edge_width)))
        edge_widthRA = max(edge_raA, edge_raB)

        box_north = SimpleBox(raA, raB, decB - edge_width, decB)
        box_east = SimpleBox(raA, raA + edge_widthRA, decA + edge_width, decB - edge_width)
        box_west = SimpleBox(raB - edge_widthRA, raB, decA + edge_width, decB - edge_width)
        box_south = SimpleBox(raA, raB, decA, decA + edge_width)
        box_middle = SimpleBox(box_east.raB, box_west.raA, box_south.decB, box_north.decA)
        entire_box = SimpleBox(raA, raB, decA, decB)

        print(f"&&&chunk={chunk_id}")
        print(f"&&& edge_widthRA={edge_widthRA} edge_raA={edge_raA} edge_raB={edge_raB}")
        print(f"&&&  north={box_north}")
        print(f"&&&  south={box_south}")
        print(f"&&&  east ={box_east}")
        print(f"&&&  west ={box_west}")
        print(f"&&&  mid  = {box_middle}")
        print(f"&&&  entire={entire_box}")

        edge_boxes = [box_north, box_east, box_west, box_south]
        for bx in edge_boxes:
            if not entire_box.contains(bx):
                raise RuntimeError(f"box={bx} is not contained by entire_box={entire_box}")
        if not entire_box.contains(box_middle):
            raise RuntimeError(f"box_mid={box_middle} is not contained by entire_box={entire_box}")
        edge_area = sum(x.area() for x in edge_boxes)
        entire_area = entire_box.area()

        ratio_edge_to_entire = edge_area/entire_area

        boxes = []
        if(ratio_edge_to_entire > 0.90 or edge_width <= 0.0):
            boxes.append(entire_box)
        else:
            boxes.extend(edge_boxes)
            if(not edge_only):
                boxes.append(box_middle)

        return boxes

    def _generate_table_block(self, box, column_generators, row_count, unique_box_id,
                              box_center, prereq_tables=None, edge_only=False, **kwargs):

        output_columns = {}

        for column_name, column_generator in column_generators.items():
            print(f"Working on column_name={column_name}")
            split_column_names = column_name.split(",")
            for name in split_column_names:
                output_columns[name] = []

            block = column_generator(
                box, row_count, self.seed,
                box_center=box_center,
                unique_box_id=unique_box_id,
                prereq_tables=prereq_tables,
                edge_only=edge_only)
            self._add_to_list(block, output_columns, split_column_names)

        return output_columns


