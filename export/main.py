import yaml
import duckdb

from gcs import create_parquet_from_db, upload_file_to_gcs


def main():
    with open("config.YAML", "r") as file:
        config = yaml.safe_load(file)

    try:
        gcs_bucket_name = config["gcs"]["bucket_name"]
        gcs_folder_path = config["gcs"]["folder_path"]
        gcs_blob_name = config["gcs"]["blob_name"]

        duckdb_table_name = config["duckdb"]["table_name"]
        
    except Exception as e:
        print(e)

    with duckdb.connect("/data/database.db") as con:
        parquet_fpath = create_parquet_from_db(con, duckdb_table_name)

    result = upload_file_to_gcs(parquet_fpath, gcs_bucket_name, gcs_folder_path, gcs_blob_name)

    print(f"sucess: {result}")

main()