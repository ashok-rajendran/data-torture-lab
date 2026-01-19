import functions_framework
from google.cloud import storage
import os

storage_client = storage.Client()
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
SIZE_THRESHOLD_BYTES = 2 * 1024 * 1024 #2MB


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def process_gcs_file(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    size_str = data.get("size", '0')
    size = int(size_str)

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Size: {size} bytes")

    if size <= SIZE_THRESHOLD_BYTES:
        print("File Size <= 2MB, No Action Item needed")
        return

    source_bucket = storage_client.bucket(bucket)
    dest_bucket = storage_client.bucket(DESTINATION_BUCKET)
    source_blob = source_bucket.blob(name)
    DESTINATION_PATH_PREFIX = "bigfiles/"
    dest_blob_name = DESTINATION_PATH_PREFIX + name[len("src/"):]

    try:
        source_bucket.copy_blob(source_blob, dest_bucket, dest_blob_name)
        print(f"Copied {name} to {DESTINATION_BUCKET}/{dest_blob_name}")
    except Exception as e:
        print(f"Error Copying {name} to {DESTINATION_BUCKET}: {e}")
