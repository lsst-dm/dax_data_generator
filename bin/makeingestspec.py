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


import getopt
import json
import os
import sys
import yaml


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


def check_spec(table, schema_columns, gen_config):
    """Check that the column names in 'spec' match those in the schema.

    Parameters
    ----------
    table : string
        The table whose columns are being compared
    schema : dictionary
        Column information taken the schema.
    gen_config : string
        Name of the configuration file used by the data generator.

    Returns
    -------
    success : bool
        True if no errors were found.

    Note
    ----
        The configuration file for the data generator is created by
    hand at this point while the schema file comes from an autorative
    source. The data generator configuration file should be fixed to
    match the schema.
    """
    spec_globals = {}
    with open(gen_config, 'r') as file:
        gen_config_contents = file.read()
    exec(gen_config_contents, spec_globals)
    assert 'spec' in spec_globals, "Specification file must define a variable 'spec'."
    spec = spec_globals['spec']
    if table not in spec:
        print(f"table {table} not found in spec")
        return True
    spec_cols = spec[table]['columns']
    gen_cols = []
    for key in spec_cols:
        cols = key.split(",")
        gen_cols.extend(cols)
    success = True
    count = 0
    for gen, schema in zip(gen_cols, schema_columns):
        if gen.split(':')[0] != schema['name']:
            print(f"Error column name mismatch table={table} generator={gen} schema={schema} count={count}")
            success = False
            break
        count += 1
    if len(gen_cols) != len(schema_columns):
        print(f"Error length mismatch generator table={table} config={len(gen_cols)} schema={len(schema_columns)}")
        success = False
    return success


def convert_database(database_name, base_path, gen_config):
    """
    Loads a Felis-formated schema file for the specified database, and
    adds column information from that schema to the ingest and partitioner
    configuration files.
    """

    sdm_filename = os.path.join(base_path, f"{database_name}.yaml")
    gen_config = os.path.join(base_path, gen_config)

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
        # Create files useful for building fakeGenSpec.py
        with open(f"tmp_cols_{table_name}", "w") as ft:
            for column in sdm_tables[table_name]['columns']:
                if table_name == 'Source':
                    ft.write(f"{column['name']}:{column['mysql:datatype']},")
                else:
                    ft.write(f"{column['name']},")
        # Check spec colums against schema_columns
        if not check_spec(table_name, schema_columns, gen_config):
            print("Error, fix data generator configuration", gen_config)
            exit(1)

        template_filename = os.path.join(base_path, f"ingestCfgs/{database_name}_{table_name}_template.json")
        output_filename = os.path.join(base_path, f"ingestCfgs/{database_name}_{table_name}.json")
        transform_ingest_json(template_filename, output_filename, schema_columns)

    # Partitioner configuration
    for table_name in sdm_tables.keys():
        template_filename = os.path.join(base_path, f"partitionerCfgs/{table_name}_template.cfg")
        output_filename = os.path.join(base_path, f"partitionerCfgs/{table_name}.cfg")

        schema_columns = [column["name"] for column in sdm_tables[table_name]['columns']]

        transform_partitioner_json(template_filename, output_filename, schema_columns)


def usage():
    print('-h, --help  help')
    print('-c, --config   data generator configuration file name. default="fakeGenSpec.py')
    print('-d, --database  Name of the database.')
    print('-p, --path path of the working directory. defaul="localConfig"')


if __name__ == '__main__':

    argumentList = sys.argv[1:]
    print("argumentList=", argumentList)
    options = "hd:p:"
    long_options = ["help", "config", "database", "path"]
    database_name = None
    gen_config = "fakeGenSpec.py"
    path = "localConfig"
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        print("arguments=", arguments)
        for arg, val in arguments:
            if arg in ("-h", "--help"):
                usage()
                exit(0)
            elif arg in ("-c", "--config"):
                gen_config = val
            elif arg in ("-d", "--database"):
                database_name = val
            elif arg in ("-p", "--path"):
                path = val
    except getopt.error as err:
        print(str(err))
        exit(1)
    if database_name is None:
        print("Error. No database name provided, exiting.")
        exit(1)

    print('converting files for database=', database_name, "in", path)
    convert_database(database_name, path, gen_config)

