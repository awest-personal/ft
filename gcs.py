from google.cloud import storage



def write_to_gcs(bucket_name, contents, destination_blob_name, storage_client): 
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    return f"gs://{bucket_name}./{destination_blob_name}"

    