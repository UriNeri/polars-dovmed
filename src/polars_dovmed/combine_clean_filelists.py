"""
Combine the pmc_oa *file_lists.csv files from NCBI FTP and convert to a parquet.

Usage:
python src/polars_dovmed/combine_clean_filelists.py \
    --input-dir "data/pubmed_central/pmc_oa/*/*filelist.csv" \
        --output data/pubmed_central/pmc_oa/filelists.parquet \
        --log-file logs/combine_clean_filelists.log 
"""

import argparse
import sys
from pathlib import Path

import polars as pl
from rich.console import Console

from polars_dovmed.utils import normalize_column_name, setup_logging

console = Console(
    width=None,
    force_terminal=sys.stdout.isatty(),
    legacy_windows=False,
    no_color=not sys.stdout.isatty(),
)


def main(input_dir: str, output_path: str, log_file: str) -> None:
    # Set up logging
    logger = setup_logging(log_file=log_file)
    logger.info(f"Combining file lists from {input_dir} and saving to {output_path}")

    try:
        # Read CSV files with schema inference
        file_lists_df = pl.scan_csv(
            input_dir, glob=True, schema_overrides={"PMID": pl.Utf8}
        ).collect()
        logger.info(f"Read {len(file_lists_df)} rows from file lists")

        # Clean and normalize column names
        file_lists_df = file_lists_df.rename(normalize_column_name)
        logger.info(f"Renamed columns: {file_lists_df.columns}")

        # Split article_file column into collection and pmc_id
        file_lists_df = file_lists_df.with_columns(
            pl.col("article_file")
            .str.split_exact("/", 1)
            .struct.rename_fields(["collection", "pmc_id"])
            .alias("fields")
        ).unnest("fields")

        # Clean pmc_id and drop article_file
        file_lists_df = file_lists_df.with_columns(
            pl.col("pmc_id").str.strip_suffix(suffix=".xml").alias("pmc_id")
        ).drop("article_file")

        # Handle empty pmid values
        file_lists_df = file_lists_df.with_columns(
            pl.when(pl.col("pmid").is_in(["0", ""]))
            .then(None)
            .otherwise(pl.col("pmid"))
            .alias("pmid")
        )

        # Write to parquet
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_lists_df.write_parquet(path)
        logger.info(f"Saved to {path} with schema: {file_lists_df.schema}")

    except Exception as e:
        logger.error(f"Error combining file lists: {e}")
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine pmc_oa *file_lists.csv files and convert to parquet"
    )
    parser.add_argument(
        "--input-dir", required=True, help="Glob pattern for input CSV files"
    )
    parser.add_argument("--output", required=True, help="Output parquet file path")
    parser.add_argument("--log-file", required=True, help="Log file path")
    args = parser.parse_args()

    main(args.input_dir, args.output, args.log_file)
