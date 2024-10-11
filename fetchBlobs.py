
import os
import csv
from azure.storage.blob import BlobServiceClient, BlobPrefix

connection_string = "<SAS HERE>"
container_name = "<CONTAINER HERE>"

# Path to the output CSV file
output_csv_path = "<EXPORT CSV PATH HERE>"

print("Starting the blob listing script")

def list_blobs_recursive(container_client, path):
    """Recursively list blobs in the container and their sizes."""
    blobs = container_client.walk_blobs(name_starts_with=path)
    for blob in blobs:
        if isinstance(blob, BlobPrefix):
            yield from list_blobs_recursive(container_client, blob.name)
        else:
            yield blob

def bytes_to_mb(size_in_bytes):
    """Convert bytes to megabytes."""
    return size_in_bytes / (1024 * 1024)

try:
    # Create the BlobServiceClient object
    print("Creating BlobServiceClient")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the container client object
    print(f"Getting container client for container: {container_name}")
    container_client = blob_service_client.get_container_client(container_name)

    # List the blobs in the container recursively
    print("Listing blobs in the container recursively")
    blob_list = list_blobs_recursive(container_client, "")

    # Write the blob names and sizes to a CSV file
    print(f"Writing blob names and sizes to {output_csv_path}")
    with open(output_csv_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Blob Name", "Blob Size (MB)"])
        for blob in blob_list:
            size_mb = bytes_to_mb(blob.size)
            #logging.info(f"Found blob: {blob.name}, Size: {size_mb:.2f} MB")
            writer.writerow([blob.name, f"{size_mb:.2f}"])

    # Check the size of the exported CSV file
    file_size = os.path.getsize(output_csv_path)
    print(f"Exported CSV file size: {file_size} bytes")

    print(f"Finished writing blob names and sizes to {output_csv_path}")

except Exception as e:
    print(f"An error occurred: {e}")

print("Script completed")