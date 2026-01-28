import argparse
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

import polars as pl
import requests


def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("pmc_xml_processor.download")


def download_filelists(output_dir: Path, subsets: List[str] = ["oa_comm"]) -> List[str]:
    PMC_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk"
    PMC_SUBSETS = {
        "oa_comm": f"{PMC_FTP_BASE}/oa_comm/xml/",
        "oa_noncomm": f"{PMC_FTP_BASE}/oa_noncomm/xml/",
        "oa_other": f"{PMC_FTP_BASE}/oa_other/xml/",
    }
    base_urls = [PMC_SUBSETS.get(subset, "") for subset in subsets]
    xmltargz_files = []
    csv_urls = []
    for base_url in base_urls:
        response = requests.get(base_url)
        response.raise_for_status()
        new_csv_urls = [
            base_url + fname
            for fname in re.findall(r'href="([^"]+\.csv)"', response.text)
        ]
        csv_urls = csv_urls + new_csv_urls
        new_urls = [
            base_url + fname
            for fname in re.findall(r'href="([^"]+\.tar\.gz)"', response.text)
        ]
        xmltargz_files = xmltargz_files + new_urls
    lazy_frames = [pl.scan_csv(url) for url in csv_urls]
    combined_lazy_frame = pl.concat(lazy_frames, how="vertical_relaxed").collect()
    combined_lazy_frame.write_parquet(output_dir / "filelists.parquet")
    return xmltargz_files


def download_pmc(
    subsets: List[str] = ["oa_comm"],
    output_dir: Path | str = Path.cwd(),
    max_connections: int = 5,
    verbose: bool = False,
):
    logger = setup_logging(verbose)
    output_dir = Path(output_dir)

    # Create parent directory for PMC data
    pmc_parent_dir = output_dir / "pmc_oa"
    pmc_parent_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created PMC parent directory: {pmc_parent_dir}")

    for subset in subsets:
        subset_dir = pmc_parent_dir / subset
        subset_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Processing subset: {subset}")

        files = download_filelists(subset_dir, [subset])
        logger.info(f"Found {len(files)} files to download for {subset}")

        successful_downloads = 0
        start_time = time.time()
        total_bytes_downloaded = 0

        with ThreadPoolExecutor(max_workers=max_connections) as executor:
            futures = [
                executor.submit(download_file, url, subset_dir, logger) for url in files
            ]
            for future in as_completed(futures):
                result = future.result()
                if result[0]:  # success
                    successful_downloads += 1
                    total_bytes_downloaded += result[1]  # bytes downloaded

        elapsed_time = time.time() - start_time
        avg_speed = total_bytes_downloaded / elapsed_time if elapsed_time > 0 else 0
        logger.info(
            f"Download for {subset} completed. {successful_downloads}/{len(files)} files downloaded successfully. Average speed: {avg_speed / 1024 / 1024:.2f} MB/second"
        )


def download_file(url: str, output_dir: Path, logger: logging.Logger):
    filepath = output_dir / Path(url).name
    bytes_downloaded = 0
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # total_size = int(response.headers.get("content-length", 0))
        with open(filepath, "wb") as f:
            for data in response.iter_content(chunk_size=1024):
                bytes_downloaded += len(data)
                f.write(data)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        return False, 0

    return True, bytes_downloaded


def main():
    """Main CLI entry point for downloading PMC data."""
    parser = argparse.ArgumentParser(description="Download PMC XML files.")
    parser.add_argument(
        "subsets",
        nargs="+",
        help="PMC subsets to download (oa_comm, oa_noncomm, oa_other)",
    )
    parser.add_argument(
        "-o", "--output_dir", default=Path.cwd(), help="Output directory"
    )
    parser.add_argument(
        "-c",
        "--max_connections",
        type=int,
        default=5,
        help="Maximum parallel downloads",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    args = parser.parse_args()

    download_pmc(
        subsets=args.subsets,
        output_dir=args.output_dir,
        max_connections=args.max_connections,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
