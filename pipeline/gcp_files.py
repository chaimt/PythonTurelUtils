import asyncio
import json
import logging
import os
import sys
import time
from typing import Optional
from urllib.parse import unquote

import requests
from global_config import GlobalConfig
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from retry_policy import RETRY_POLICY, SkipRetry
from tqdm import tqdm

logger = logging.getLogger(__name__)


def generate_path(expert_id, account_id, session_date_str):
    return f"sessions/A-{account_id}/{session_date_str}/E-{expert_id}"


@RETRY_POLICY
def file_exists_in_bucket(gcp_file_path: str):
    blob = GlobalConfig().bucket.blob(gcp_file_path)
    return blob.exists()


@RETRY_POLICY
def upload_string(text: str, gcp_file_path: str):
    blob = GlobalConfig().bucket.blob(gcp_file_path)
    blob.upload_from_string(text)


@RETRY_POLICY
def upload_json_string(bucket: Bucket, data: str, gcp_file_path: str):
    blob = bucket.blob(gcp_file_path)
    blob.upload_from_string(json.dumps(data), content_type="application/json")


@RETRY_POLICY
def download_string(gcp_file_path: str):
    blob = GlobalConfig().bucket.blob(gcp_file_path)
    if not blob.exists():
        return None
    return blob.download_as_text()


@RETRY_POLICY
def upload_file(file_path: str, gcp_file_path: str):
    # Get file size for progress bar
    file_size = os.path.getsize(file_path)
    blob = GlobalConfig().bucket.blob(gcp_file_path)

    upload_start_time = time.time()
    # Open file and wrap with tqdm progress bar
    if logger.getEffectiveLevel() <= logging.DEBUG:
        with open(file_path, "rb") as f, tqdm.wrapattr(f, "read", total=file_size, desc="Uploading to GCS", bytes=True, file=sys.stdout) as file_obj:
            blob.upload_from_file(file_obj, size=file_size)
    else:
        with open(file_path, "rb") as f:
            blob.upload_from_file(f, size=file_size)

    upload_end_time = time.time()
    upload_time = upload_end_time - upload_start_time
    logger.debug(f"✅ Upload completed in {upload_time:.2f} seconds for file: {gcp_file_path}")


@RETRY_POLICY
def download_file(gcp_file_path: str, local_path: str, bucket: Bucket = None):
    gcp_file_path = unquote(gcp_file_path)
    start_time = time.time()
    if not bucket:
        bucket = GlobalConfig().bucket
    blob = bucket.blob(gcp_file_path)
    if not blob.exists():
        raise SkipRetry(f"File not found in GCP: {gcp_file_path}")
    blob.reload()
    if logger.getEffectiveLevel() <= logging.DEBUG:
        blob_size = blob.size
        # Handle case where blob.size might be None
        if blob_size is None:
            logger.warning(f"⚠️ Blob size is None for {gcp_file_path}, downloading without progress bar")
            blob.download_to_filename(local_path)
        else:
            with tqdm(total=blob_size, unit="B", unit_scale=True, desc=f"Downloading {os.path.basename(gcp_file_path)}", file=sys.stdout) as pbar:
                blob.download_to_filename(local_path)
                pbar.update(blob_size)
    else:
        blob.download_to_filename(local_path)

    end_time = time.time()
    download_time = end_time - start_time
    file_name = os.path.basename(local_path)
    logger.debug(f"✅ Download completed in {download_time:.2f} seconds for file: {file_name}")


@RETRY_POLICY
def download_and_upload_file_to_gcp(bucket, download_url, download_token, path):
    # Create a temporary local file path
    file_name = os.path.basename(path)
    file_name = file_name.split("&")[0] if "&" in file_name else file_name
    temp_file_path = f"/tmp/{file_name}"

    # Download file locally first
    logger.debug(f"📥 Downloading locally to {temp_file_path}")
    file_name = os.path.basename(download_url)
    download_start_time = time.time()
    try:
        try:
            if download_url.startswith("https://storage.googleapis.com"):
                # Extract bucket name and path from GCS URL
                gs_url = download_url.replace("https://storage.googleapis.com/", "")
                bucket_name, *path_parts = gs_url.split("/")
                path_in_bucket = "/".join(path_parts)

                # Initialize GCS client and get source bucket
                storage_client = storage.Client()
                source_bucket = storage_client.bucket(bucket_name)

                # Download file from source bucket
                download_file(path_in_bucket, temp_file_path, source_bucket)
                total_size = os.path.getsize(temp_file_path)
            else:
                if download_token:
                    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {download_token}"}
                else:
                    headers = {"Content-Type": "application/json"}

                logger.debug(f"📥 Downloading Media data from {download_url} and uploading to GCS path: {path}...")

                with requests.get(download_url, stream=True, headers=headers) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    with open(temp_file_path, "wb") as f:

                        if logger.getEffectiveLevel() <= logging.DEBUG:
                            with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {file_name}", file=sys.stdout) as pbar:
                                for chunk in response.iter_content(chunk_size=1024 * 1024):
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                        else:
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
        except Exception as e:
            logger.error(f"❌ Error downloading file: {e}")
            raise e

        download_end_time = time.time()
        download_time = download_end_time - download_start_time
        logger.debug(f"✅ Download completed in {download_time:.2f} seconds for file: {file_name}")

        upload_start_time = time.time()
        try:
            with open(temp_file_path, "rb") as f:
                blob = bucket.blob(path)
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    with tqdm.wrapattr(f, "read", total=total_size, desc="Uploading to GCS", bytes=True, file=sys.stdout) as wrapped:
                        blob.upload_from_file(wrapped, size=total_size, content_type="video/mp4")
                else:
                    blob.upload_from_file(f, size=total_size, content_type="video/mp4")
            logger.debug(f"✅ Uploaded to GCS {path}")

            upload_end_time = time.time()
            upload_time = upload_end_time - upload_start_time
            logger.debug(f"✅ Upload completed in {upload_time:.2f} seconds")

            total_time = upload_end_time - download_start_time
            logger.debug(f"⏱️ Total operation time: {total_time:.2f} seconds (Download: {download_time:.2f}s, Upload: {upload_time:.2f}s)")
        except Exception as e:
            logger.error(f"❌ Error uploading file: {e}")
            raise e
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def download_with_progress(bucket_name: str, source_blob_name: str, destination_file_name: str, credentials_path: Optional[str] = None) -> bool:
    """
    Download a file from GCS with progress logging

    Args:
        bucket_name: Name of the GCS bucket
        source_blob_name: Name of the file in the bucket
        destination_file_name: Local path where file will be saved
        credentials_path: Optional path to service account JSON file

    Returns:
        bool: True if download successful, False otherwise
    """
    try:
        # Initialize the client
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            client = storage.Client()
        else:
            # Uses default credentials (from gcloud auth or environment)
            client = storage.Client()

        # Get the bucket and blob
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Check if blob exists
        if not blob.exists():
            logger.error(f"❌ File {source_blob_name} does not exist in bucket {bucket_name}")
            return False

        # Get file size for progress calculation
        blob.reload()
        total_size = blob.size
        if total_size is None:
            logger.error(f"❌ Could not determine file size for {source_blob_name}")
            return False
        logger.debug(f"📥 Starting download of {source_blob_name} ({total_size:,} bytes)")

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(destination_file_name) if os.path.dirname(destination_file_name) else ".", exist_ok=True)

        # Download with progress tracking
        start_time = time.time()

        with open(destination_file_name, "wb") as f:
            # Download in chunks to track progress
            chunk_size = 1024 * 1024  # 1MB chunks

            # Use download_as_bytes with chunk_size for progress tracking
            with blob.open("rb") as blob_file:
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    with tqdm(
                        total=total_size, unit="B", unit_scale=True, desc=f"Downloading {os.path.basename(source_blob_name)}", file=sys.stdout
                    ) as pbar:
                        while True:
                            chunk = blob_file.read(chunk_size)
                            if not chunk:
                                break

                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    while True:
                        chunk = blob_file.read(chunk_size)
                        if not chunk:
                            break

                        f.write(chunk)

        end_time = time.time()
        total_time = end_time - start_time
        avg_speed = total_size / total_time / 1024 / 1024  # MB/s

        logger.debug(f"✅ Downloaded {os.path.basename(source_blob_name)}. Total time: {total_time:.2f} seconds, Average speed: {avg_speed:.2f} MB/s")

        return True

    except Exception as e:
        logger.error(f"❌ Downloaded {os.path.basename(source_blob_name)}. Download failed: {str(e)}")
        return False


async def download_file_async(gcp_file_path: str, local_path: str, bucket: Bucket = None, heartbeat_callback=None):
    """
    Async version of download_file that downloads a file from GCS to local storage.

    Args:
        gcp_file_path: Path to the file in GCS bucket
        local_path: Local file path where the file should be saved
        bucket: Optional GCS bucket object (uses GlobalConfig if not provided)
        heartbeat_callback: Optional callback function to report progress (for Temporal heartbeats)

    Raises:
        SkipRetry: If file not found in GCP
    """
    import asyncio

    import aiofiles

    gcp_file_path = unquote(gcp_file_path)
    start_time = time.time()
    if not bucket:
        bucket = GlobalConfig().bucket
    blob = bucket.blob(gcp_file_path)
    if not blob.exists():
        raise SkipRetry(f"File not found in GCP: {gcp_file_path}")
    blob.reload()

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(local_path) if os.path.dirname(local_path) else ".", exist_ok=True)

    if logger.getEffectiveLevel() <= logging.DEBUG:
        blob_size = blob.size
        # Handle case where blob.size might be None
        if blob_size is None:
            logger.warning(f"⚠️ Blob size is None for {gcp_file_path}, downloading without progress bar")
            # Run sync download in thread
            await asyncio.to_thread(blob.download_to_filename, local_path)
        else:
            # Download in chunks asynchronously
            chunk_size = 1024 * 1024  # 1MB chunks
            downloaded_bytes = 0
            heartbeat_interval = 10 * 1024 * 1024  # Send heartbeat every 10MB

            async with aiofiles.open(local_path, "wb") as f:
                with tqdm(total=blob_size, unit="B", unit_scale=True, desc=f"Downloading {os.path.basename(gcp_file_path)}", file=sys.stdout) as pbar:
                    # Use blob.open() for streaming download
                    with blob.open("rb") as blob_file:
                        while True:
                            # Read chunk in thread to avoid blocking
                            chunk = await asyncio.to_thread(blob_file.read, chunk_size)
                            if not chunk:
                                break
                            await f.write(chunk)
                            pbar.update(len(chunk))
                            downloaded_bytes += len(chunk)

                            # Send heartbeat every 10MB if callback provided
                            if heartbeat_callback and downloaded_bytes % heartbeat_interval < chunk_size:
                                progress = (downloaded_bytes / blob_size * 100) if blob_size > 0 else 0
                                heartbeat_callback(
                                    {
                                        "stage": "downloading_from_gcs",
                                        "downloaded_bytes": downloaded_bytes,
                                        "total_bytes": blob_size,
                                        "progress_percent": round(progress, 2),
                                    }
                                )
    else:
        # Run sync download in thread
        await asyncio.to_thread(blob.download_to_filename, local_path)

    end_time = time.time()
    download_time = end_time - start_time
    file_name = os.path.basename(local_path)
    logger.debug(f"✅ Download completed in {download_time:.2f} seconds for file: {file_name}")


async def download_and_upload_file_to_gcp_async(bucket, download_url, download_token, path, heartbeat_callback=None):
    # Create a temporary local file path
    file_name = os.path.basename(path)
    file_name = file_name.split("&")[0] if "&" in file_name else file_name
    file_extention = file_name.split(".")[-1].lower()
    if file_extention == "mp4":
        content_type = "video/mp4"
    elif file_extention == "m4a":
        content_type = "audio/m4a"
    elif file_extention == "wav":
        content_type = "audio/wav"
    elif file_extention == "json":
        content_type = "application/json"
    elif file_extention == "txt":
        content_type = "text/plain"
    else:
        content_type = "application/octet-stream"
    temp_file_path = f"/tmp/{file_name}"

    # Download file locally first
    logger.debug(f"📥 Downloading locally to {temp_file_path}")
    file_name = os.path.basename(download_url)
    blob = bucket.blob(path)
    if not blob.exists():
        download_start_time = time.time()
        try:
            try:
                if download_url.startswith("https://storage.googleapis.com"):
                    # Extract bucket name and path from GCS URL
                    gs_url = download_url.replace("https://storage.googleapis.com/", "")
                    bucket_name, *path_parts = gs_url.split("/")
                    path_in_bucket = "/".join(path_parts)

                    # Initialize GCS client and get source bucket
                    storage_client = storage.Client()
                    source_bucket = storage_client.bucket(bucket_name)

                    # Download file from source bucket
                    await download_file_async(path_in_bucket, temp_file_path, source_bucket, heartbeat_callback)
                    total_size = os.path.getsize(temp_file_path)
                else:
                    if download_token:
                        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {download_token}"}
                    else:
                        headers = {"Content-Type": "application/json"}

                    logger.debug(f"📥 Downloading Media data from {download_url} and uploading to GCS path: {path}...")

                    with requests.get(download_url, stream=True, headers=headers) as response:
                        response.raise_for_status()
                        total_size = int(response.headers.get("content-length", 0))
                        with open(temp_file_path, "wb") as f:

                            if logger.getEffectiveLevel() <= logging.DEBUG:
                                with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {file_name}", file=sys.stdout) as pbar:
                                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                                        f.write(chunk)
                                        pbar.update(len(chunk))
                            else:
                                for chunk in response.iter_content(chunk_size=1024 * 1024):
                                    f.write(chunk)
            except Exception as e:
                logger.error(f"❌ Error downloading file: {e}")
                raise e

            download_end_time = time.time()
            download_time = download_end_time - download_start_time
            logger.debug(f"✅ Download completed in {download_time:.2f} seconds for file: {file_name}")

            upload_start_time = time.time()
            try:
                blob = bucket.blob(path)
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    with open(temp_file_path, "rb") as f:
                        with tqdm.wrapattr(f, "read", total=total_size, desc="Uploading to GCS", bytes=True, file=sys.stdout) as wrapped:
                            # Run the blocking upload in a thread pool
                            await asyncio.to_thread(blob.upload_from_file, wrapped, size=total_size, content_type=content_type)
                else:
                    with open(temp_file_path, "rb") as f:
                        # Run the blocking upload in a thread pool
                        await asyncio.to_thread(blob.upload_from_file, f, size=total_size, content_type=content_type)
                logger.debug(f"✅ Uploaded to GCS {path}")

                upload_end_time = time.time()
                upload_time = upload_end_time - upload_start_time
                logger.debug(f"✅ Upload completed in {upload_time:.2f} seconds")

                total_time = upload_end_time - download_start_time
                logger.debug(f"⏱️ Total operation time: {total_time:.2f} seconds (Download: {download_time:.2f}s, Upload: {upload_time:.2f}s)")
                return (path, "Uploaded")

            except Exception as e:
                logger.error(f"❌ Error uploading file: {e}")
                raise e
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    else:
        logger.info(f"🔍 File already exists in GCS: {path}")
        return (path, "Exists")


def main():
    """Example usage"""
    # Configuration
    BUCKET_NAME = "ritual-sessions-dev"
    SOURCE_BLOB_NAME = "sessions/A-4652/2025/04/24/08/45/E-34/34.mp4"
    DESTINATION_FILE_NAME = "/tmp/34.mp4"
    CREDENTIALS_PATH = None  # Optional: path to service account JSON file

    # Download the file
    success = download_with_progress(
        bucket_name=BUCKET_NAME, source_blob_name=SOURCE_BLOB_NAME, destination_file_name=DESTINATION_FILE_NAME, credentials_path=CREDENTIALS_PATH
    )

    if success:
        print("✅ Download completed successfully!")
    else:
        print("❌ Download failed. Check the log file for details.")


if __name__ == "__main__":
    main()
