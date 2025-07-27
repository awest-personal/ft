import os
import logging
import duckdb

from google.cloud import storage


def create_parquet_from_db(con: duckdb.DuckDBPyConnection, table_name: str) -> str:
    """
    Exports a duckdb table to a temporary parquet file, stored locally. 
    """

    if not table_name:
        raise ValueError("haven't provided the right table name")

    local_fpath = f"{table_name}_temp.parquet"

    try:
        logging.info(f"exporting table {table_name} to {local_fpath}")
        con.sql(f"COPY {table_name} TO {local_fpath} (FORMAT PARQUET);")
        logging.info("export successful")
        return local_fpath
    
    except duckdb.Error as e:
        logging.error(f"failed to export {table_name}: {e}")
        raise e


def upload_file_to_gcs(storage_client: storage.Client, local_fpath: str, bucket_name: str, gcs_path: str):
    """
    Uploads a local file to GCS.
    """

    if not os.path.exists(local_fpath):
        raise FileNotFoundError(f"local file not found at {local_fpath}")

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_fpath)
        logging.info("upload successful")
        return gcs_path
    
    except Exception as e:
        logging.error(f"failed to upload to gcs: {e}")
        raise

    finally:
        os.remove(local_fpath) # this might not always get called, should probs fix that
