
def create_duck_db_table_creation_string_from_schema(schema, table_name, exclusions):
    type_mapping = {
    'int64': 'BIGINT',
    'string': 'VARCHAR',
    'float64': 'DOUBLE',
    'boolean': 'BOOLEAN'}

    column_definitions = []

    for column_name, column_type in schema["columns"].items():
        if column_name not in exclusions:
            duckdb_type = type_mapping[column_type]
            column_definitions.append(f"{column_name} {duckdb_type}")

    columns_sql = ", ".join(column_definitions)
    return f"CREATE OR REPLACE TABLE {table_name} ({columns_sql})"


def create_duck_db_table_insertion_string_from_schema(schema, exclusions):

    duckdb_columns = []
    for column_name in schema["columns"].keys():
        if column_name not in exclusions:
            duckdb_columns.append(column_name)
    
    columns_sql = ", ".join(duckdb_columns)
    return columns_sql
        



