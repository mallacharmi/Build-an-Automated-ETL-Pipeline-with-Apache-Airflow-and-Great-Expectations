"""
Raw data downloader module.
Downloads the raw E-commerce dataset via HTTP/HTTPS with error handling,
standard logging, and verified destination writing.
"""

import logging
import os
import requests

# Configure module-level logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )


def download_raw_data(url: str, dest_path: str, timeout: int = 60) -> None:
    """
    Download a raw dataset from a remote URL to a local destination file.

    Args:
        url (str): The HTTP/HTTPS endpoint to download the dataset from.
        dest_path (str): The local destination file path.
        timeout (int): Timeout in seconds for the network request.

    Raises:
        ValueError: If url or dest_path are invalid or empty.
        requests.RequestException: If the network request fails or returns an HTTP error code (e.g., 404, 500).
        IOError: If writing to the destination path fails.
    """
    logger.info("Starting raw data ingestion task.")
    logger.info("Source URL: %s", url)
    logger.info("Target destination: %s", dest_path)

    if not url:
        raise ValueError("Download URL cannot be empty or None.")
    if not dest_path:
        raise ValueError("Destination path cannot be empty or None.")

    dest_dir = os.path.dirname(dest_path)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)
        logger.info("Ensured directory exists: %s", dest_dir)

    try:
        logger.info("Initiating HTTP GET request with %s second timeout...", timeout)
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        total_bytes = 0
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)

        file_size_mb = total_bytes / (1024 * 1024)
        logger.info(
            "Successfully downloaded %s (%.2f MB, %d bytes) to %s",
            url,
            file_size_mb,
            total_bytes,
            dest_path,
        )

        if not os.path.exists(dest_path) or os.path.getsize(dest_path) == 0:
            raise IOError(f"Downloaded file {dest_path} is missing or empty.")

    except requests.exceptions.HTTPError as http_err:
        logger.error("HTTP error encountered while downloading from %s: %s", url, http_err, exc_info=True)
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error("Network or connection error during raw data download: %s", req_err, exc_info=True)
        raise
    except Exception as exc:
        logger.error("Unexpected error in raw data downloader: %s", exc, exc_info=True)
        raise

    logger.info("Raw data ingestion completed successfully.")


if __name__ == "__main__":
    import sys
    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://raw.githubusercontent.com/Gaelim/google-data-stuido/main/Ecommerce_Data.csv"
    test_dest = sys.argv[2] if len(sys.argv) > 2 else "data/raw/ecommerce_data.csv"
    download_raw_data(test_url, test_dest)
