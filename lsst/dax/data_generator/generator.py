
import pandas as pd

__all__ = ["DataGenerator"]


class DataGenerator:

    def __init__(self, spec):
        self.spec = spec
        self.tables = spec.keys()

    def make_chunk(self, chunk_id, num_rows=None):
        output_tables = {}

        for table in self.tables:
            column_generators = self.spec[table]["columns"]
            prereq_rows = self.spec[table].get("prereq_row", None)
            prereq_tables = self.spec[table].get("prereq_tables", [])
            output_columns = {}
            for column_name, column_generator in column_generators.items():

                if prereq_rows is None:
                    output_data = column_generator(chunk_id, num_rows,
                                                   prereq_tables={t: output_tables[t] for t in prereq_tables})
                else:
                    prereq_table_contents = {t: output_tables[t] for t in prereq_tables}
                    output_data = column_generator(chunk_id, num_rows,
                                                   prereq_row = output_tables[prereq_rows].iloc[1],
                                                   prereq_tables=prereq_table_contents)

                split_column_names = column_name.split(",")
                if isinstance(output_data, tuple) or isinstance(output_data, list):
                    for i, name in enumerate(split_column_names):
                        output_columns[name] = output_data[i]
                else:
                    if(len(split_column_names) > 1):
                        assert ValueError, "Column name implies multiple returns, but generator returned one"

                    output_columns[column_name] = output_data
                output_tables[table] = pd.DataFrame(output_columns)

            return output_tables
