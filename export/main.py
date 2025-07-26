import yaml
import duckdb
import logging
import sys

from google.cloud import storage
from gcs import create_parquet_from_db, upload_file_to_gcs


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # config stuff
    try: 
        with open("config.YAML", "r") as file:
            config = yaml.safe_load(file)

        gcs_bucket_name = config["gcs"]["bucket_name"]
        gcs_folder_path = config["gcs"]["folder_path"]
        gcs_blob_name = config["gcs"]["blob_name"]
        gcs_full_path = f"{gcs_folder_path}/{gcs_blob_name}"
        db_path = "/data/database.db"
        duckdb_table_name = config["duckdb"]["table_name"]
        logging.info("all config loaded ok")


        # gcs stuff
        storage_client = storage.Client.create_anonymous_client() 
        
        with duckdb.connect(db_path) as con:
            parquet_fpath = create_parquet_from_db(con, duckdb_table_name)

        result = upload_file_to_gcs(storage_client, parquet_fpath, gcs_bucket_name, gcs_full_path)
        print(f"sucess: {result}")
        
    except Exception as e:
        logging.error(f"export pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()