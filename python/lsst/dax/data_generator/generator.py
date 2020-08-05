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


import numpy as np
import pandas as pd

from collections import defaultdict
from . import columns

__all__ = ["DataGenerator"]


class TableColumnInfo:
    def __init__(self, colNames, generator, position):
        self.colNames = colNames
        self.position = position
        self.generator = generator
        self.block = None

    def __repr__(self):
        return ("{colNames:" + self.colNames + ' position:' + str(self.position)
              + " generator:" + str(self.generator) + ' block:' + str(self.block))

class DataGenerator:
    """Create a DataGenerator based on a specification dictionary.
    The specification dictionary describes all of the tables and their
    constituent columns to generate. Nothing is generated at
    initialization; calls must be made to DataGenerator.make_chunk() to
    synthesize data.

    No validation is performed on the generator specification.
    """

    def __init__(self, spec):

        self.spec = spec
        self.tables = spec.keys()

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

        The structure is output_columns[table_name] = list(np.array([]))

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
                assert ValueError, ("Column name implies multiple returns, "
                                    "but generator only returned one")

    def make_chunk(self, chunk_id, num_rows=None, seed=1, edge_width=0.017, edge_only=False):
        """Generate synthetic data for one chunk.

        Parameters
        ----------
        chunk_id : int
            ID of the chunk to generate.
        num_rows : int or dict
            Generate the specified number of rows. Can either be a
            scalar, or a dictionary of the form {table_name: num_rows}.
        seed : int
            Random number seed
        edge_width : float
            Width/height of the edge in degrees
        edge_only : bool
            When True, only generate objects within edge_width of the edge
            of the chunk. When False, create all objects in the chunk.

        Returns
        -------
        dictionary of pandas.DataFrames
            The output dictionary contains each generated table as a
            pandas.DataFrame.

        """

        output_tables = {}
        if isinstance(num_rows, dict):
            rows_per_table = dict(num_rows)
        else:
            rows_per_table = defaultdict(lambda: num_rows)

        resolvedOrder = self._resolve_table_order(self.spec)
        tableColumns = dict()
        newRowsPerTable = dict()

        for table in resolvedOrder:
            tableLength = rows_per_table[table]
            columnSpecs = self.spec[table]["columns"]
            cols = list()
            position = 0
            for col, generator in columnSpecs.items():
                colNames = col
                colInfo = TableColumnInfo(colNames, generator, position)
                position += 1
                # RaDecGenerator needs to run first to determine the
                # length of the table.
                if isinstance(generator, columns.RaDecGenerator):
                    colInfo.block = colInfo.generator(chunk_id, tableLength, seed, edge_width, edge_only)
                    blockLength = len(colInfo.block[0])
                    if tableLength != blockLength:
                        # Reducing length to what was generated for RA and Dec
                        tableLength = blockLength
                cols.append(colInfo)
            tableColumns[table] = cols
            rows_per_table[table] = tableLength

        for table in resolvedOrder:
            cols = tableColumns[table]
            prereq_rows = self.spec[table].get("prereq_row", None)
            prereq_tables = self.spec[table].get("prereq_tables", [])
            output_columns = {}
            for colInfo in cols:

                split_column_names = colInfo.colNames.split(",")
                for name in split_column_names:
                    output_columns[name] = []

                if prereq_rows is None:
                    if colInfo.block is None:
                        prereq_tbls = {t: output_tables[t] for t in prereq_tables}
                        colInfo.block = colInfo.generator(
                            chunk_id, rows_per_table[table], seed,
                            prereq_tables=prereq_tbls)
                    self._add_to_list(colInfo.block, output_columns, split_column_names)
                else:
                    prereq_table_contents = {t: output_tables[t] for t in prereq_tables}
                    for n in range(len(output_tables[prereq_rows])):
                        preq_rw = output_tables[prereq_rows].iloc[n]
                        colInfo.block = colInfo.generator(
                            chunk_id, rows_per_table[table], seed,
                            prereq_row=preq_rw,
                            prereq_tables=prereq_table_contents)
                        self._add_to_list(colInfo.block, output_columns, split_column_names)

            for name in output_columns.keys():
                temp = np.concatenate(output_columns[name])
                output_columns[name] = temp
            print("rows_per_table=", rows_per_table)

            output_tables[table] = pd.DataFrame(output_columns)

        return output_tables

