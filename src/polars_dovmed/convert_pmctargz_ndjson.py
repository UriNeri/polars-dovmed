"""
Convert .tar.gz files from local PMC OA to NDJSON batches.

This script processes tar.gz files directly from PMC OA without extracting
all individual files to disk at once, instead converting them to NDJSON format in batches.

Usage:
python src/polars_dovmed/convert_pmctargz_ndjson.py \
    --pmc-oa-dir data/pubmed_central// \
    --ndjson-dir data/pubmed_central//ndjson_files/ \
    --batch-size 5000 \
    --max-workers 6 \
    --subset-types oa_comm oa_noncomm oa_other \
    --verbose --log-file logs/convert_pmctargz_ndjson.log
"""

import argparse
import logging
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

from polars_dovmed import xml_processor
from polars_dovmed.utils import setup_logging

console = Console(
    width=None,
    force_terminal=sys.stdout.isatty(),
    legacy_windows=False,
    no_color=not sys.stdout.isatty(),
)


def process_single_tar_file(
    tar_file: Path, worker_dir: Path, batch_size: int, logger: logging.Logger
) -> Tuple[List[str], int]:
    """Process a single tar.gz file and write to batched NDJSON files in worker directory"""
    ndjson_files = []
    total_processed = 0
    current_batch = []
    batch_count = 0

    # Create temporary directory for this tar file's XML extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        try:
            with tarfile.open(tar_file, "r:gz") as tar:
                members = tar.getmembers()
                logger.debug(
                    f"Worker processing {tar_file.name} with {len(members)} files"
                )

                for member in members:
                    if member.name.endswith(".xml"):
                        # Extract XML content to memory
                        pmc_id = extract_pmc_id_from_path(member.name)
                        if pmc_id:
                            extracted_file = tar.extractfile(member)
                            if extracted_file:
                                xml_content = extracted_file.read()

                                # Write to temporary file for batch processing
                                temp_xml_file = temp_path / f"{pmc_id}.xml"
                                with open(temp_xml_file, "wb") as f:
                                    f.write(xml_content)

                                current_batch.append(str(temp_xml_file))

                                # Process batch when it reaches the specified size
                                if len(current_batch) >= batch_size:
                                    batch_count += 1
                                    ndjson_file = (
                                        worker_dir / f"batch_{batch_count:04d}.ndjson"
                                    )

                                    processed_count = (
                                        xml_processor.nxml.batch_xml_to_ndjson(
                                            current_batch, str(ndjson_file)
                                        )
                                    )  # type: ignore# type: ignore
                                    ndjson_files.append(str(ndjson_file))
                                    total_processed += processed_count

                                    # logger.debug(f"Worker created {ndjson_file.name} with {processed_count} records")

                                    # Clear current batch
                                    current_batch = []

                # Process any remaining files in the last batch
                if current_batch:
                    batch_count += 1
                    ndjson_file = worker_dir / f"batch_{batch_count:04d}.ndjson"

                    processed_count = xml_processor.nxml.batch_xml_to_ndjson(  # type: ignore
                        current_batch, str(ndjson_file)
                    )  # type: ignore
                    ndjson_files.append(str(ndjson_file))
                    total_processed += processed_count

                    logger.debug(
                        f"Worker created final {ndjson_file.name} with {processed_count} records"
                    )

            logger.debug(
                f"Worker processed {total_processed} XML files from {tar_file.name} into {len(ndjson_files)} batches"
            )
            return ndjson_files, total_processed

        except Exception as e:
            logger.error(f"Error processing {tar_file}: {e}")
            return [], 0


def process_tar_gz_to_ndjson_batches(
    pmc_oa_dir: str,
    output_dir: str,
    subset_types: Optional[List[str]] = None,
    batch_size: int = 50000,
    max_workers: int = 4,
    logger: logging.Logger = logging.getLogger(__name__),
) -> List[str]:
    """Process XML files from tar.gz archives to batched NDJSON files using parallel workers with separate directories"""

    if subset_types is None:
        subset_types = ["oa_comm", "oa_noncomm", "oa_other"]

    pmc_path = Path(pmc_oa_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    all_ndjson_files = []
    total_processed = 0

    # Collect all tar files
    all_tar_files = []
    for subset_type in subset_types:
        subset_dir = pmc_path / subset_type
        if not subset_dir.exists():
            logger.debug(f"subset_dir does not exist: {subset_dir}")
            continue

        tar_files = list(subset_dir.glob("*.tar.gz"))
        logger.info(f"Found {len(tar_files)} tar.gz files in subset: {subset_type}")
        all_tar_files.extend(tar_files)

    logger.info(
        f"Processing {len(all_tar_files)} tar.gz files with {max_workers} workers"
    )
    logger.info(
        f"Each worker will create batches of {batch_size} XMLs in separate directories"
    )

    # Create worker directories
    worker_dirs = []
    for i in range(max_workers):
        worker_dir = output_path / f"worker_{i:02d}"
        worker_dir.mkdir(exist_ok=True)
        worker_dirs.append(worker_dir)

    # Process files with parallel processing and progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TextColumn("({task.completed}/{task.total} archives)"),
        TextColumn("XMLs: {task.fields[xml_count]}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Processing archives to batched NDJSON files",
            total=len(all_tar_files),
            xml_count=0,
        )

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Assign tar files to workers in round-robin fashion
            future_to_info = {}
            for i, tar_file in enumerate(all_tar_files):
                worker_idx = i % max_workers
                worker_dir = worker_dirs[worker_idx]

                future = executor.submit(
                    process_single_tar_file, tar_file, worker_dir, batch_size, logger
                )
                future_to_info[future] = (tar_file, worker_idx)

            # Process completed tasks as they finish
            for future in as_completed(future_to_info):
                tar_file, worker_idx = future_to_info[future]
                try:
                    ndjson_files, xml_count = future.result()

                    all_ndjson_files.extend(ndjson_files)
                    total_processed += xml_count

                    # Update progress
                    progress.update(task, advance=1, xml_count=total_processed)

                    logger.debug(
                        f"Worker {worker_idx} finished {tar_file.name}: {xml_count} XMLs in {len(ndjson_files)} batches"
                    )

                except Exception as e:
                    logger.error(f"Error processing results from {tar_file}: {e}")
                    continue

    logger.info(
        f"Successfully processed {total_processed} XML files into {len(all_ndjson_files)} NDJSON batch files"
    )
    logger.info(f"Results organized in {len(worker_dirs)} worker directories")
    return all_ndjson_files


def extract_pmc_id_from_path(file_path: str) -> str:
    """Extract PMC ID from file path."""
    # Typical path might be like: PMC123456.xml or some/path/PMC123456.xml
    path_parts = Path(file_path).stem

    # Look for PMC pattern
    import re

    pmc_match = re.search(r"PMC\d+", path_parts)
    if pmc_match:
        return pmc_match.group(0)

    return ""


def main():
    parser = argparse.ArgumentParser(
        description="NXML to NDJSON Converter - Convert XML files from local PMC OA tar.gz archives to batched NDJSON files using parallel workers.",
        epilog="""
Usage:
    python src/polars_dovmed/convert_pmctargz_ndjson.py \
        --pmc-oa-dir data/pubmed_central/pmc_oa/ \
        --ndjson-dir data/pubmed_central/ndjson_files/ \
        --batch-size 50000 \
        --max-workers 8 \
        --subset-types oa_comm oa_noncomm oa_other \
        --verbose --log-file logs/convert_pmctargz_ndjson.log
        
    Output structure:
        ndjson_files/
        ├── worker_00/
        │   ├── batch_0001.ndjson
        │   └── batch_0002.ndjson
        ├── worker_01/
        │   └── batch_0001.ndjson
        └── worker_02/
            ├── batch_0001.ndjson
            └── batch_0002.ndjson
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--pmc-oa-dir",
        type=str,
        required=True,
        help="Directory containing the local PMC OA collection",
    )
    parser.add_argument(
        "--ndjson-dir",
        type=str,
        required=True,
        help="Directory to store NDJSON files (organized in worker subdirectories)",
    )

    # Optional arguments
    parser.add_argument(
        "--subset-types",
        nargs="+",
        default=["oa_comm", "oa_noncomm", "oa_other"],
        help="PMC OA subset types to process (default: oa_comm oa_noncomm oa_other)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Number of XMLs per NDJSON batch file (default: 50000)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel workers for processing tar.gz files (default: 4)",
    )
    parser.add_argument(
        "--log-file", type=str, help="Path to log file for saving detailed logs"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose, log_file=args.log_file)

    logger.info(
        "⚙️ NXML to NDJSON Processing Configuration",
    )
    for arg, value in vars(args).items():
        if value is not None:
            logger.info(f"{arg.replace('_', ' ').capitalize()}: {value}")

    try:
        # Validate input directory exists
        pmc_oa_path = Path(args.pmc_oa_dir)
        if not pmc_oa_path.exists():
            raise FileNotFoundError(
                f"PMC OA directory does not exist: {args.pmc_oa_dir}"
            )

        # Check if there are any subset directories
        subset_found = False
        for subset_type in args.subset_types:
            subset_dir = pmc_oa_path / subset_type
            if subset_dir.exists():
                tar_files = list(subset_dir.glob("*.tar.gz"))
                if tar_files:
                    subset_found = True
                    break

        if not subset_found:
            raise FileNotFoundError(
                f"No tar.gz files found in any subset directories: {args.subset_types}"
            )

        logger.info(f"Found PMC OA subsets in: {args.pmc_oa_dir}")

        # Process to batched NDJSON files with worker directories
        logger.info(
            f"\n**Processing tar.gz files in {args.pmc_oa_dir} to batched NDJSON files with {args.max_workers} parallel workers...**"
        )
        ndjson_files = process_tar_gz_to_ndjson_batches(
            pmc_oa_dir=args.pmc_oa_dir,
            output_dir=args.ndjson_dir,
            subset_types=args.subset_types,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            logger=logger,
        )

        # Summary
        logger.info(
            f"✅ NDJSON processing completed successfully! "
            f"Created {len(ndjson_files)} NDJSON batch files in {args.max_workers} worker directories under {args.ndjson_dir}"
        )

        if args.verbose:
            # Show sample of created files
            sample_files = [Path(f).name for f in ndjson_files[:5]]
            logger.info(f"Sample NDJSON files: {sample_files}")
            if len(ndjson_files) > 5:
                logger.info(f"... and {len(ndjson_files) - 5} more files")

            # Show worker directory structure
            worker_dirs = list(Path(args.ndjson_dir).glob("worker_*"))
            for worker_dir in worker_dirs[:3]:  # Show first 3 workers
                batch_files = list(worker_dir.glob("*.ndjson"))
                logger.info(f"{worker_dir.name}: {len(batch_files)} batch files")

    except Exception as e:
        logger.error(f"NDJSON processing failed: {e}", exc_info=args.verbose)
        logger.info(f"**❌ NDJSON processing failed: {e}**")
        sys.exit(1)


if __name__ == "__main__":
    main()
