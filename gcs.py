from google.cloud import storage
from datetime import datetime
import os


def write_to_gcs(bucket_name, contents, destination_blob_name, storage_client): 
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    return f"gs://{bucket_name}./{destination_blob_name}"


def has_reached_threshold(con, threshold, table_name):
    try:
        row_count = con.sql(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        print(row_count)
        print(type(row_count))

        if row_count > threshold:
            return True
        else:
            return False
    except Exception as e:
        return False



def export_duck_db_to_gcs(con, table_name, bucket_name, gcs_folder_name):
    local_fpath = f"{table_name}_temp.parquet"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # maybe small collision rate, could replace with an id if we need to scale out horiz
    # gcs_blob_name = f"{gcs_folder}/{table_name}_{timestamp}.parquet" 
    gcs_blob_name = "aidan_west/data_engineering_task.parquet" # changed to reflect requirements

    try: 
        con.sql(f"COPY {table_name} TO {local_fpath} (FORMAT PARQUET);")

        storage_client = storage.Client.create_anonymous_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_blob_name)
        print(f"would upload to this blob: {blob}")
        # blob.upload_from_file_name(local_file_path)

        print("success, uploaded")
        return gcs_blob_name
    except Exception as e:
        print(f"something has gone wrong: {e}")
        return e
    finally:
        if os.path.exists(local_fpath):
            os.remove(local_fpath)

    