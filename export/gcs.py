from google.cloud import storage
from datetime import datetime
import os


def create_parquet_from_db(con, table_name):
    local_fpath = f"{table_name}_temp.parquet"

    try:
        con.sql(f"COPY {table_name} TO {local_fpath} (FORMAT PARQUET);")
        return local_fpath
    except Exception as e:
        return e


def upload_file_to_gcs(local_fpath, bucket_name, folder_path, blob_name):
    try:

        storage_client = storage.Client.create_anonymous_client()
        bucket = storage_client.bucket(bucket_name)
        fully_qualified_gcs_file_path = f"{folder_path}/{blob_name}"
        blob = bucket.blob(fully_qualified_gcs_file_path)

        blob.upload_from_filename(local_fpath)
        return f"uploaded {blob} to {bucket} at {fully_qualified_gcs_file_path}"
    except Exception as e:
        return e

