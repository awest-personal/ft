import duckdb
import pandas as pd

def create_table_from_schema(con: duckdb.DuckDBPyConnection, config: dict):
    """
    Creates a duckdb table, given the necessary columns. 
    """
    type_mapping = {
    'int64': 'BIGINT',
    'string': 'VARCHAR',
    'float64': 'DOUBLE',
    'boolean': 'BOOLEAN'}

    column_definitions = []

    table_name = config["duckdb"]["table_name"]
    schema = config["duckdb"]["schema"]

    for column_name, column_type in schema.items():
        duckdb_type = type_mapping[column_type]
        column_definitions.append(f"{column_name} {duckdb_type}")

    if not column_definitions:
        raise ValueError("no columns for the table")

    columns_sql = ", ".join(column_definitions)
    con.sql(f"CREATE OR REPLACE TABLE {table_name} ({columns_sql});")


def insert_data_into_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame):
    """
    Inserts data from a pandas dataframe into a duckdb table.
    """
    con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM df")

