
import numpy as np
import pandas as pd
from collections import defaultdict

__all__ = ["DataGenerator"]


class DataGenerator:

    def __init__(self, spec):
        self.spec = spec
        self.tables = spec.keys()

    @staticmethod
    def _resolve_table_order(spec):
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
        if isinstance(generated_data, tuple) or isinstance(generated_data, list):
            for i, name in enumerate(split_column_names):
                output_columns[name].append(generated_data[i])
        else:
            output_columns[split_column_names[0]].append(generated_data)
            if(len(split_column_names) > 1):
                assert ValueError, "Column name implies multiple returns, but generator returned one"

    def make_chunk(self, chunk_id, num_rows=None):
        output_tables = {}
        if isinstance(num_rows, dict):
            rows_per_table = dict(num_rows)
        else:
            rows_per_table = defaultdict(lambda: num_rows)

        for table in self._resolve_table_order(self.spec):
            column_generators = self.spec[table]["columns"]
            prereq_rows = self.spec[table].get("prereq_row", None)
            prereq_tables = self.spec[table].get("prereq_tables", [])
            output_columns = {}
            for column_name, column_generator in column_generators.items():

                split_column_names = column_name.split(",")
                for name in split_column_names:
                    output_columns[name] = []

                if prereq_rows is None:
                    individual_obj = column_generator(chunk_id, rows_per_table[table],
                                                      prereq_tables={t: output_tables[t] for t in prereq_tables})
                    self._add_to_list(individual_obj, output_columns, split_column_names)
                else:
                    prereq_table_contents = {t: output_tables[t] for t in prereq_tables}
                    for n in range(len(output_tables[prereq_rows])):
                        individual_obj = column_generator(chunk_id, rows_per_table[table],
                                                          prereq_row=output_tables[prereq_rows].iloc[n],
                                                          prereq_tables=prereq_table_contents)
                        self._add_to_list(individual_obj, output_columns, split_column_names)

            for name in output_columns.keys():
                temp = np.concatenate(output_columns[name])
                output_columns[name] = temp

            output_tables[table] = pd.DataFrame(output_columns)

        return output_tables
