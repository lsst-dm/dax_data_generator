#!/usr/bin/env python
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


import yaml
import json
import os


def transform_ingest_json(template_filename, output_filename, schema_columns):
    """
    Reads in a json file from template_filename, and inserts into the schema
    section a list of the columns provided in schema_columns. The template file
    must contain an entry of {"name": "PLACEHOLDER"}, which will be replaced by
    the contents of schema_columns.
    """

    try:
        with open(template_filename) as f:
            input_json = json.load(f)
    except FileNotFoundError:
        print("Could not locate template file {:s}, skipping.".format(template_filename))
        return

    output_json = input_json.copy()

    for n in range(len(output_json["schema"])):
        if(output_json["schema"][n]["name"] == "PLACEHOLDER"):
            output_json["schema"][n:n+1] = schema_columns
        break

    print("Writing out {:s}".format(output_filename))
    with open(output_filename, "w") as f:
        json.dump(output_json, f, indent=4)


def transform_partitioner_json(template_filename, output_filename, schema_columns):
    """
    Reads in a json file from template_filename, and inserts into the schema
    section a list of the columns provided in schema_columns. The template file
    must contain an entry of "PLACEHOLDER", which will be replaced by the
    contents of schema_columns. Any other columns entries in the template are
    preserved.
    """

    try:
        with open(template_filename) as f:
            input_json = json.load(f)
    except FileNotFoundError:
        print("Could not locate template file {:s}, skipping.".format(template_filename))
        return

    output_json = input_json.copy()

    output_field = output_json["out"]["csv"]["field"]
    for n in range(len(output_field)):
        if(output_field[n] == "PLACEHOLDER"):
            output_field[n:n+1] = schema_columns

    input_field = output_json["in"]["csv"]["field"]
    for n in range(len(input_field)):
        if(input_field[n] == "PLACEHOLDER"):
            input_field[n:n+1] = schema_columns

    print("Writing out {:s}".format(output_filename))
    with open(output_filename, "w") as f:
        json.dump(output_json, f, indent=4)


def convert_database(database_name):
    """
    Loads a Felis-formated schema file for the specified database, and
    adds column information from that schema to the ingest and partitioner
    configuration files.
    """

    base_path = database_name
    sdm_filename = os.path.join(database_name, f"{database_name}.yaml")

    with open(sdm_filename) as f:
        sdm_schema = yaml.load(f.read(), Loader=yaml.FullLoader)

    sdm_tables = {schema['name']: schema for schema in sdm_schema['tables']}

    # Ingest configuration
    for table_name in sdm_tables.keys():

        schema_columns = []
        for column in sdm_tables[table_name]['columns']:

            if("mysql:datatype" in column):
                type_string = column["mysql:datatype"]
            else:
                raise RuntimeError("Missing mysql:datatype field for column {:s}".format(column['name']))
            if("nullable" in column and (column["nullable"] is False)):
                type_string += " NOT NULL"

            schema_columns.append({"name": column['name'],
                                   "type": type_string})

        template_filename = os.path.join(base_path, f"ingestCfgs/{database_name}_{table_name}_template.json")
        output_filename = os.path.join(base_path, f"ingestCfgs/{database_name}_{table_name}.json")
        transform_ingest_json(template_filename, output_filename, schema_columns)

    # Partitioner configuration
    for table_name in sdm_tables.keys():
        template_filename = os.path.join(base_path, f"partitionerCfgs/{table_name}_template.cfg")
        output_filename = os.path.join(base_path, f"partitionerCfgs/{table_name}.cfg")

        schema_columns = [column["name"] for column in sdm_tables[table_name]['columns']]

        transform_partitioner_json(template_filename, output_filename, schema_columns)


if __name__ == '__main__':

    for database in ['kpm50']:
        convert_database(database)


