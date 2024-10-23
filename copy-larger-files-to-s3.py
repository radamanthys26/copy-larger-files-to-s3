import hashlib
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from concurrent.futures import ThreadPoolExecutor
import time

def calculate_md5(file_path):
    """Calculates the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def upload_part_with_retries(s3_client, bucket_name, s3_key, part_number, data, upload_id, max_retries=3, delay=2):
    """Uploads a part with retries."""
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Uploading part {part_number} (attempt {attempt + 1})...", flush=True)
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=data
            )
            print(f"Part {part_number} uploaded successfully.", flush=True)
            return {'ETag': part['ETag'], 'PartNumber': part_number}
        except Exception as e:
            attempt += 1
            print(f"Error uploading part {part_number} (attempt {attempt}): {e}", flush=True)
            if attempt < max_retries:
                print(f"Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)  # Wait before retrying
            else:
                print(f"Failed to upload part {part_number} after {max_retries} attempts.", flush=True)
                raise

def upload_file_multipart(bucket_name, file_path, s3_key, max_workers=5, max_retries=3):
    """Uploads a file to S3 using multipart upload, with parallel uploads and retries."""
    s3_client = boto3.client('s3')

    # Initiates the multipart upload
    try:
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return

    parts = []
    part_number = 1
    part_hashes = []  # To store part hashes
    chunk_size = 30 * 1024 * 1024  # Size of each part (30 bytes * 1024 * 1024 = 30MB)

    try:
        # Using ThreadPoolExecutor for simultaneous uploads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(chunk_size)  # Reads parts of the file
                    if not data:
                        break

                    # Upload parts in parallel with retries
                    futures.append(
                        executor.submit(upload_part_with_retries, s3_client, bucket_name, s3_key, part_number, data, multipart_upload['UploadId'], max_retries=max_retries)
                    )
                    
                    # Calculate the MD5 of the part
                    part_hash = hashlib.md5(data).hexdigest()
                    part_hashes.append(part_hash)

                    part_number += 1

            # Collect the results of the uploads
            for future in futures:
                parts.append(future.result())

        # Completes the upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=multipart_upload['UploadId'],
            MultipartUpload={'Parts': parts}
        )

        print(f"Upload completed: {s3_key}", flush=True)

    except Exception as e:
        # Abort the upload if an error occurs
        s3_client.abort_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=multipart_upload['UploadId']
        )
        print(f"Error during upload: {e}", flush=True)

    return part_hashes  # Returns the part hashes

def check_file_integrity(bucket_name, file_path, s3_key, part_hashes):
    """Checks the integrity of the uploaded file on S3."""
    local_md5 = calculate_md5(file_path)

    s3_client = boto3.client('s3')
    response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
    etag = response['ETag'].strip('"')  # Removes quotes

    # If the upload was multipart, we need to consider the ETag
    if '-' in etag:  # Check if the ETag has a dash, indicating multipart
        # Combine the part hashes
        combined_hash = hashlib.md5()
        for part_hash in part_hashes:
            combined_hash.update(bytes.fromhex(part_hash))

        # The ETag for multipart uploads is the MD5 hash of the parts and the number of parts
        final_etag = combined_hash.hexdigest()

        if final_etag == etag.split('-')[0]:
            print("The files are identical.")
        else:
            print("The files are different.")
    else:
        # For simple uploads, compare directly
        if local_md5 == etag:
            print("The files are identical.")
        else:
            print("The files are different.")

# Usage
bucket_name = 'BUCKET-NAME'
file_path = './FILE.TXT'
s3_key = 'FILE.TXT'

# Upload the file with parallel multipart upload and retries, and get part hashes
part_hashes = upload_file_multipart(bucket_name, file_path, s3_key, max_workers=5, max_retries=3)

# Verify integrity after upload
check_file_integrity(bucket_name, file_path, s3_key, part_hashes)
