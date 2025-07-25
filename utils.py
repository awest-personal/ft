import yaml
import duckdb

def create_duckdb_table_from_config():
    # to do this


def map_type_pandas_duckdb(input_type):
    type_mapping = {
    'int64': 'BIGINT',
    'string': 'VARCHAR',
    'float64': 'DOUBLE',
    'boolean': 'BOOLEAN'}

    output_type = type_mapping.get(input_type)
    return output_type

