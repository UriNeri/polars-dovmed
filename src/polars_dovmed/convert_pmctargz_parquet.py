"""
Convert .tar.gz files from local PMC OA directly to Parquet files.

This script processes tar.gz files directly from PMC OA containing XML files and converting them to Parquet files in batches.
Note, this is a "lossy" conversion - some of the XML tags are lost in this process.

Usage:
python src/polars_dovmed/convert_pmctargz_parquet.py \
    --pmc-oa-dir data/pubmed_central/pmc_oa/ \
    --parquet-dir data/pubmed_central/parquet_files/ \
    --batch-size 5000 \
    --max-workers 6 \
    --subset-types oa_comm oa_noncomm oa_other \
    --verbose --log-file logs/convert_pmctargz_parquet.log
"""

import argparse
import logging
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl
from rich.console import Console

# from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

from polars_dovmed import xml_processor
from polars_dovmed.utils import setup_logging

console = Console(
    width=None,
    force_terminal=sys.stdout.isatty(),
    legacy_windows=False,
    no_color=not sys.stdout.isatty(),
)


def assign_files_to_workers(
    tar_files: List[Path], max_workers: int, logger: Optional[logging.Logger] = None
) -> List[Tuple[Path, int]]:
    """Assign tar files to workers based on file size (greedy load balancing).
    Returns a list of (tar_file, worker_index) tuples.
    """
    # Get sizes
    file_sizes = [(tar, tar.stat().st_size) for tar in tar_files]
    # Sort descending by size so we allocate biggest files first
    file_sizes.sort(key=lambda x: x[1], reverse=True)

    # Initialize load per worker
    worker_loads = [0] * max_workers
    assignments: List[Tuple[Path, int]] = []

    for tar, size in file_sizes:
        # Find worker with smallest current load
        worker_idx = worker_loads.index(min(worker_loads))
        assignments.append((tar, worker_idx))
        worker_loads[worker_idx] += size

        if logger:
            logger.debug(f"Assigned {tar.name} ({size} bytes) to worker {worker_idx}")

    # Log load distribution
    if logger:
        total_load = sum(worker_loads)
        logger.info(f"Load distribution across {max_workers} workers:")
        for i, load in enumerate(worker_loads):
            percentage = (load / total_load * 100) if total_load > 0 else 0
            logger.info(f"  Worker {i:2d}: {load:>15,} bytes ({percentage:>5.1f}%)")

    return assignments


def process_single_tar_file(
    tar_file: Path, worker_dir: Path, batch_size: int, logger: logging.Logger
) -> Tuple[List[str], int]:
    """Process a single tar.gz file and write to batched Parquet files in worker directory"""
    parquet_files = []
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
                                    parquet_file = (
                                        worker_dir / f"batch_{batch_count:04d}.parquet"
                                    )

                                    processed_count = process_xml_batch_to_parquet(
                                        current_batch, parquet_file, logger
                                    )
                                    parquet_files.append(str(parquet_file))
                                    total_processed += processed_count

                                    # Clear current batch
                                    current_batch = []

                # Process any remaining files in the last batch
                if current_batch:
                    batch_count += 1
                    parquet_file = worker_dir / f"batch_{batch_count:04d}.parquet"

                    processed_count = process_xml_batch_to_parquet(
                        current_batch, parquet_file, logger
                    )
                    parquet_files.append(str(parquet_file))
                    total_processed += processed_count

                    logger.debug(
                        f"Worker {worker_dir} created final {parquet_file.name} with {processed_count} records"
                    )

            logger.debug(
                f"Worker processed {total_processed} XML files from {tar_file.name} into {len(parquet_files)} batches"
            )
            return parquet_files, total_processed

        except Exception as e:
            logger.error(f"Error processing {tar_file}: {e}")
            return [], 0


def process_xml_batch_to_parquet(
    xml_file_paths: List[str], output_parquet: Path, logger: logging.Logger
) -> int:
    """Process a batch of XML files and convert directly to Parquet"""
    try:
        # Convert XML files to Polars DataFrame
        df = xml_processor.nxml.xml_to_polars(xml_file_paths)  # type: ignore

        if df.is_empty():
            logger.warning(f"No data extracted from batch, skipping {output_parquet}")
            return 0

        # Apply schema normalization and data cleaning
        df = normalize_and_clean_dataframe(df, logger)

        # Create output directory if it doesn't exist
        output_parquet.parent.mkdir(parents=True, exist_ok=True)

        # Write to Parquet
        df.write_parquet(output_parquet)

        record_count = len(df)
        logger.debug(f"Created {output_parquet.name} with {record_count} records")

        return record_count

    except Exception as e:
        logger.error(f"Error processing XML batch to {output_parquet}: {e}")
        return 0


def normalize_and_clean_dataframe(
    df: pl.DataFrame, logger: logging.Logger
) -> pl.DataFrame:
    """Normalize DataFrame schema and clean data"""

    # Define unified schema
    unified_schema = {
        "pmid": pl.Utf8,
        "pmc_id": pl.Utf8,
        "title": pl.Utf8,
        "abstract_text": pl.Utf8,
        "authors": pl.Utf8,
        "journal": pl.Utf8,
        "publication_date": pl.Utf8,
        "doi": pl.Utf8,
        "full_text": pl.Utf8,
        "file_path": pl.Utf8,
    }

    # Create type casting expressions based on unified schema
    type_casting_exprs = []

    for col, target_dtype in unified_schema.items():
        if col in df.columns:
            current_dtype = df[col].dtype

            # Skip if types are already compatible
            if current_dtype == target_dtype:
                continue

            # Handle NULL columns that might have values
            if current_dtype == pl.Null:
                logger.debug(f"Converting NULL column {col} to {target_dtype}")
                if target_dtype == pl.Utf8:
                    type_casting_exprs.append(pl.col(col).cast(pl.Utf8).fill_null(""))
                elif target_dtype == pl.Int64:
                    type_casting_exprs.append(pl.col(col).cast(pl.Int64).fill_null(0))
                elif target_dtype == pl.Float64:
                    type_casting_exprs.append(
                        pl.col(col).cast(pl.Float64).fill_null(0.0)
                    )
                else:
                    type_casting_exprs.append(pl.col(col).cast(target_dtype))
                continue

            # Handle List types - convert to comma-separated strings
            if isinstance(current_dtype, pl.List) or target_dtype == pl.Utf8:
                if isinstance(current_dtype, pl.List):
                    # Convert List to comma-separated string
                    type_casting_exprs.append(
                        pl.col(col)
                        .list.unique()
                        .drop_nulls()
                        .list.join(", ")
                        .cast(pl.Utf8)
                        .fill_null("")
                    )
                else:
                    # Convert to string and fill nulls
                    type_casting_exprs.append(pl.col(col).cast(pl.Utf8).fill_null(""))
            elif target_dtype == pl.Int64:
                # Cast to integer and fill nulls with 0
                type_casting_exprs.append(pl.col(col).cast(pl.Int64).fill_null(0))
            elif target_dtype == pl.Float64:
                # Cast to float and fill nulls with 0.0
                type_casting_exprs.append(pl.col(col).cast(pl.Float64).fill_null(0.0))
            else:
                # For other types, just cast
                type_casting_exprs.append(pl.col(col).cast(target_dtype))

    # Apply type casting
    if type_casting_exprs:
        df = df.with_columns(type_casting_exprs)

    # Add missing columns with default values
    missing_columns = set(unified_schema.keys()) - set(df.columns)
    for col in missing_columns:
        target_dtype = unified_schema[col]
        if target_dtype == pl.Utf8:
            df = df.with_columns(pl.lit("").alias(col))
        elif target_dtype == pl.Int64:
            df = df.with_columns(pl.lit(0).alias(col))
        elif target_dtype == pl.Float64:
            df = df.with_columns(pl.lit(0.0).alias(col))
        else:
            df = df.with_columns(pl.lit(None).alias(col))

    # Clean full text data
    if "full_text" in df.columns:
        df = df.with_columns(
            pl.col("full_text")
            .str.strip_prefix("1.")
            .str.strip_prefix("I.")
            .str.strip_prefix("1 ")
            .str.strip_prefix("I ")
            .str.strip_prefix(" ")
            .str.strip_prefix("Introduction")
            .str.strip_prefix("INTRODUCTION")
            .str.strip_prefix("BACKGROUND")
            .str.strip_prefix("Background")
            .str.strip_prefix(" ")
            .str.strip_prefix(":")
        )

    return df


def process_tar_gz_to_parquet_batches(
    pmc_oa_dir: str,
    output_dir: str,
    subset_types: Optional[List[str]] = None,
    batch_size: int = 5000,
    max_workers: int = 4,
    logger: logging.Logger = logging.getLogger(__name__),
) -> List[str]:
    """Process XML files from tar.gz archives to batched Parquet files using parallel workers with separate directories"""

    if subset_types is None:
        subset_types = ["oa_comm", "oa_noncomm", "oa_other"]

    pmc_path = Path(pmc_oa_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    all_parquet_files = []
    total_processed = 0
    total_bytes = 0

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
        f"Total size of all tar.gz files: {sum(f.stat().st_size for f in all_tar_files):,} bytes"
    )

    # Log file size statistics
    if all_tar_files:
        file_sizes = [f.stat().st_size for f in all_tar_files]
        logger.info(
            f"File size statistics - Min: {min(file_sizes):,} bytes, "
            f"Max: {max(file_sizes):,} bytes, "
            f"Avg: {sum(file_sizes) // len(file_sizes):,} bytes"
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
        TextColumn("Bytes: {task.fields[bytes_count]}"),
        TextColumn("({task.completed}/{task.total} archives)"),
        TextColumn("XMLs: {task.fields[xml_count]}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Processing archives to batched Parquet files",
            total=len(all_tar_files),
            xml_count=0,
            bytes_count=0,
        )

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Assign tar files to workers based on file size
            assignments = assign_files_to_workers(all_tar_files, max_workers, logger)
            future_to_info = {}
            for tar_file, worker_idx in assignments:
                worker_dir = worker_dirs[worker_idx]

                future = executor.submit(
                    process_single_tar_file, tar_file, worker_dir, batch_size, logger
                )
                future_to_info[future] = (tar_file, worker_idx)

            # Process completed tasks as they finish
            for future in as_completed(future_to_info):
                tar_file, worker_idx = future_to_info[future]
                try:
                    parquet_files, xml_count = future.result()

                    all_parquet_files.extend(parquet_files)
                    total_processed += xml_count
                    total_bytes += tar_file.stat().st_size

                    # Update progress
                    progress.update(task, advance=1, xml_count=total_processed, bytes_count=total_bytes)

                    logger.debug(
                        f"Worker {worker_idx} finished {tar_file.name}: {xml_count} XMLs in {len(parquet_files)} batches"
                    )

                except Exception as e:
                    logger.error(f"Error processing results from {tar_file}: {e}")
                    continue

    logger.info(
        f"Successfully processed {total_processed} XML files into {len(all_parquet_files)} Parquet batch files"
    )
    logger.info(f"Results organized in {len(worker_dirs)} worker directories")
    return all_parquet_files


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
        description="XML to Parquet Converter - Convert XML files from local PMC OA tar.gz archives directly to batched Parquet files using parallel workers.",
        epilog="""
Usage:
    python src/polars_dovmed/convert_pmctargz_parquet.py \
        --pmc-oa-dir data/pubmed_central// \
        --parquet-dir data/pubmed_central/parquet_files2323/ \
        --batch-size 5000 \
        --max-workers 8 \
        --subset-types oa_comm oa_noncomm oa_other \
        --verbose --log-file logs/convert_pmctargz_parquet.log
        
    Output structure:
        parquet_files/
        ├── worker_00/
        │   ├── batch_0001.parquet
        │   └── batch_0002.parquet
        ├── worker_01/
        │   └── batch_0001.parquet
        └── worker_02/
            ├── batch_0001.parquet
            └── batch_0002.parquet
        """,
    )

    # Required arguments
    parser.add_argument(
        "--pmc-oa-dir",
        type=str,
        required=True,
        help="Directory containing the local PMC OA collection",
    )
    parser.add_argument(
        "--parquet-dir",
        type=str,
        required=True,
        help="Directory to store the parquet files (organized in worker subdirectories)",
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
        default=5000,
        help="Number of XMLs per parquet file",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel workers for processing tar.gz files",
    )
    parser.add_argument(
        "--log-file", type=str, help="Path to log file for saving detailed logs"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose, log_file=args.log_file)

    logger.info(
        "⚙️ XML tar.gz to parquets Processing Configuration",
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

        # Process to batched parquet files with worker directories
        logger.info(
            f"\n**Processing tar.gz files in {args.pmc_oa_dir} to batched parquet files with {args.max_workers} parallel workers...**"
        )
        # Process to batched parquet files with worker directories
        parquet_files = process_tar_gz_to_parquet_batches(
            pmc_oa_dir=args.pmc_oa_dir,
            output_dir=args.parquet_dir,
            subset_types=args.subset_types,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            logger=logger,
        )

        # Summary
        logger.info(
            f"✅ XML tar.gz processing completed successfully! "
            f"Created {len(parquet_files)} parquet files in {args.max_workers} worker directories under {args.parquet_dir}"
        )

        if args.verbose:
            # Show sample of created files
            sample_files = [Path(f).name for f in parquet_files[:5]]
            logger.info(f"Sample Parquet files: {sample_files}")
            if len(parquet_files) > 5:
                logger.info(f"... and {len(parquet_files) - 5} more files")

            import polars as pl

            sample_df = pl.scan_parquet(parquet_files[:5]).collect()
            logger.info(f"Sample DataFrame shape: {sample_df.shape}")
            logger.info(f"Sample DataFrame columns: {sample_df.collect_schema()}")
            logger.info(f"Sample DataFrame:\n{sample_df.head(2)}")

            # Show worker directory structure
            worker_dirs = list(Path(args.parquet_dir).glob("worker_*"))
            for worker_dir in worker_dirs[:3]:  # Show first 3 workers
                batch_files = list(worker_dir.glob("*.parquet"))
                logger.info(f"{worker_dir.name}: {len(batch_files)} batch files")

    except Exception as e:
        logger.error(f"XML tar.gz processing failed: {e}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == "__main__":
    main()
