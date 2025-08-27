"""
Script to fetch, clean, and store hospital datasets from the CMS metastore.
"""

import os
import re
from datetime import date
import concurrent.futures
import json
import requests

CMS_METASTORE = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)


def get_last_successful_date() -> str:
    """
    Retrieve the date of the last successful run from the checkpoint file.
    returns:
        str: Date in 'YYYY-MM-DD' format of the last successful run, or '0001-01-01' if no checkpoint exists.
    """
    if os.path.exists("_checkpoints/last_succesfull.json"):
        with open("_checkpoints/last_succesfull.json", "r", encoding="utf-8") as f:
            checkpoint_data = json.load(f)
            return checkpoint_data.get("last_run", "0001-01-01")
    return "0001-01-01"


def fetch_hospital_datasets(modified_after: str) -> list[str]:
    """
    Fetch datasets related to hospitals from the CMS metastore that have been modified after the given date.
    args:
        modified_after (str): Date in 'YYYY-MM-DD' format.  If None, fetch all datasets.
    returns:
        list of dataset download URLs (str) for hospital datasets.
    """

    response = requests.get(CMS_METASTORE, timeout=10)
    response.raise_for_status()
    datasets = response.json()

    ma_date = date.fromisoformat(modified_after) if modified_after else date.min

    hospital_datasets = []
    for record in datasets:
        record_md = date.fromisoformat(record.get("modified", "0001-01-01"))
        if "Hospitals" in record.get("theme", []) and record_md > ma_date:
            csv_distribution = next(
                filter(
                    lambda d: d.get("mediaType") == "text/csv",
                    record.get("distribution", []),
                ),
                None,
            )
            hospital_datasets.append(csv_distribution.get("downloadURL"))

    return hospital_datasets


def snakeify_header(header: str) -> str:
    """
    Convert a header string to snake_case by removing special characters and replacing spaces with underscores.
    args:
        header (str): The header string to convert.
    returns:
        str: The converted snake_case header string.
    """
    # Remove special characters and trim whitespace
    cleaned_header = re.sub(r"[^a-zA-Z\d\s]", "", header).strip()
    # Convert to snake_case, replacing spaces with underscores and handling camelCase etc.
    snake_header = re.sub(r"(?<=[a-z])(?=[A-Z])|\s+", "_", cleaned_header).lower()
    return snake_header


def download_file(dl_url: str):
    """
    Download a file from the given URL, clean its headers, and save it locally.
    args:
        dl_url (str): The URL to download the file from.
    """
    output_file = f"cleaned_files/{dl_url.split('/')[-1]}"

    response = requests.get(dl_url, timeout=30)
    response.raise_for_status()
    with open(output_file, "w+", encoding="utf-8") as outfile:
        rows = response.text.splitlines()
        cleaned_headers = [snakeify_header(h) for h in rows[0].split(",")]
        rows[0] = ",".join(cleaned_headers)
        outfile.write("\n".join(rows))


def get_files_for_date(run_date: str):
    """
    Fetch and download hospital datasets modified after the given date.
    args:
        run_date (str): Date in 'YYYY-MM-DD' format.  If None, fetch all datasets.
    returns:
        list of concurrent.futures.Future objects representing the download tasks.
    """
    hospital_datasets = fetch_hospital_datasets(run_date)
    print(
        f"Found {len(hospital_datasets)} hospital datasets modified after {run_date}."
    )
    os.makedirs("cleaned_files", exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_results = [
            executor.submit(download_file, url) for url in hospital_datasets
        ]

    return future_results


if __name__ == "__main__":
    os.makedirs("_checkpoints", exist_ok=True)
    run_date = date.today().isoformat()
    last_run_time = get_last_successful_date()
    results = []
    try:
        results = get_files_for_date(last_run_time)
        if any(result.exception() for result in results):
            raise RuntimeError(
                "One or more downloads failed. Check checkpoint file for details."
            )
    except Exception as e:
        failed_downloads = [result for result in results if result.exception()]
        checkpoint_dict = {
            "last_run": run_date,
            "files_attempted": len(results),
            "status": "FAILED",
            "error_message": str(e),
            "download_exceptions": [str(fd.exception()) for fd in failed_downloads],
        }
        with open(
            f"_checkpoints/{run_date}_run.json", "w+", encoding="utf-8"
        ) as checkpoint_file:
            json.dump(checkpoint_dict, checkpoint_file, indent=2)
        raise e

    checkpoint_dict = {
        "last_run": run_date,
        "files_attempted": len(results),
        "status": "SUCCESS",
    }
    with open(
        f"_checkpoints/{run_date}_run.json", "w+", encoding="utf-8"
    ) as checkpoint_file:
        json.dump(checkpoint_dict, checkpoint_file, indent=2)
    with open(
        "_checkpoints/last_succesfull.json", "w+", encoding="utf-8"
    ) as checkpoint_file:
        json.dump(checkpoint_dict, checkpoint_file, indent=2)
