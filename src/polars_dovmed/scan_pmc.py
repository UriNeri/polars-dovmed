"""
This script combines the search and extraction stages into a single workflow,
(that tries to) leverages polars' lazy evaluation and parallel processing capabilities.

The workflow:
1. Load patterns from JSON files (identifiers, manuscripts, coordinates, taxonomy, databases)
2. Use scan_parquet to create lazy frames for parallel processing
3. Apply regex search expressions to find matching records
4. Extract identifiers and accessions using pattern matching expressions
5. Save results and generate summary

Usage:
python src/polars_dovmed/scan_pmc.py \
    --parquet-pattern "data/pubmed_central/pmc_oa/parquet_files/worker_*/*.parquet" \
    --queries-file primary_queries.json \
    --secondary-queries-file secondary_queries.json \
    --add-group-counts secondary \
    --extract-matches both \
    --output-path results/processed_literature \
    --verbose
"""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
from rich.panel import Panel
from rich.table import Table

from polars_dovmed.utils import (
    concept_patterns_to_regex,
    convert_nested_cols,
    create_extraction_expressions,
    drop_empty_or_null_columns,
    setup_logging,
)

consistent_schema = {
    # "pmid": pl.Utf8, # may contain nulls
    "pmc_id": pl.Utf8,
    "title": pl.Utf8,
    "abstract_text": pl.Utf8,
    "authors": pl.Utf8,  # converted from List
    "journal": pl.Utf8,
    "publication_date": pl.Utf8,
    "doi": pl.Utf8,
    "full_text": pl.Utf8,
    # "file_path": pl.Utf8,
}


def process_literature_lazy(
    parquet_pattern: str,
    primary_queries: Dict[str, List[List[str]]],
    identifier_patterns: Optional[Dict[str, List[str]]] = None,
    coordinate_patterns: Optional[Dict[str, List[str]]] = None,
    search_columns: List[str] = ["title", "abstract_text", "full_text"],
    extract_matches: Optional[str] = "primary",  # ["primary" | "secondary" | "both"]
    secondary_queries: Optional[Dict[str, List[List[str]]]] = None,
    secondary_search_columns: Optional[List[str]] = None,
    add_group_counts: Optional[str] = None,
    logger: logging.Logger = logging.getLogger(__name__),
) -> pl.DataFrame:
    """Process literature using lazy evaluation with scan_parquet and parallel collection.

    Two-stage workflow:
    2. Filter for ANY match from ANY pattern ("does this hatystake a needle in it?")
    1. Search for regex matches in text fields and collect results ("get me ALL the needles")

    """

    logger.info(f"Processing literature from pattern: {parquet_pattern}")

    # Extract disqualifying terms if present (new format: list of lists)
    disqualifying_terms = primary_queries.get("disqualifying_terms", [])
    if disqualifying_terms:
        logger.info(f"Found {len(disqualifying_terms)} disqualifying term patterns")

    # Create lazy frame from parquet files
    try:
        lazy_frame = pl.scan_parquet(
            parquet_pattern,
            glob=True,
            schema=consistent_schema,  # )
            extra_columns="ignore",
        )  # I think I dropped pmid and retreacted from most or all parquet files

        logger.info("Created lazy frame from parquet files")

    except Exception as e:
        logger.error(f"Error creating lazy frame: {e}")
        raise e

    #  Create search filter using optimized regex approach
    logger.info("Creating search filter and collecting matching records")
    queries_for_search = {
        k: v for k, v in primary_queries.items() if k != "disqualifying_terms"
    }

    # Create optimized regex patterns for each concept
    concept_regexes = []
    for concept, patterns in queries_for_search.items():
        # Check if this is a simple pattern (each pattern is a single term)
        is_simple_pattern = all(len(pattern) == 1 for pattern in patterns)

        if is_simple_pattern:
            # Simple case: just OR all the patterns together without complex logic
            simple_patterns = [pattern[0] for pattern in patterns]
            concept_regex = '|'.join(f'({pattern})' for pattern in simple_patterns)
        else:
            # Complex case: use the utility function with AND logic
            concept_regex = concept_patterns_to_regex(
                patterns, join_type="and", proximity=None
            )

        if concept_regex:
            concept_regexes.append(concept_regex)
            logger.debug(f"Concept '{concept}' regex: {concept_regex[:100]}...")

    if not concept_regexes:
        logger.error("No valid regex patterns created")
        return pl.DataFrame()

    # Combine all concept regexes with OR logic and add case-insensitive flag
    combined_regex = f"(?i)({'|'.join(concept_regexes)})"
    logger.debug(f"Combined regex length: {len(combined_regex)} characters")
    logger.debug(f"Combined regex preview: {combined_regex[:200]}...")

    # Create a single filter expression for all search columns
    column_filters = []
    for col in search_columns:
        try:
            col_filter = pl.col(col).str.contains(
                combined_regex, literal=False, strict=False
            )
            column_filters.append(col_filter)
        except Exception as e:
            logger.warning(f"Error creating filter for column '{col}': {e}")

    if not column_filters:
        logger.error("No valid column filters created")
        return pl.DataFrame()

    # OR across all search columns
    prefilter_expr = pl.reduce(lambda a, b: a | b, column_filters)
    logger.debug(f"Expression built for columns: {search_columns}")
    logger.debug(f"prefilter expressions: {prefilter_expr}")
    # logger.debug(f"Search expressions: {concept_expr}")
    start_time = time.time()
    logger.debug(f"start time:{start_time}")

    logger.info("Executing pre-search (filter) on lazy frame, THEN collecting results")
    search_lazy = lazy_frame.filter(prefilter_expr)
    search_results = pl.collect_all([search_lazy])
    search_df = pl.concat(search_results)  # might be 1 but still
    logger.info(f"Passing filter: {len(search_df)} records")

    # Apply disqualifying terms filter (OR logic, remove matches)
    if disqualifying_terms and not search_df.is_empty():
        logger.info("Applying disqualifying terms filter...")

        def disq_pattern_expr(col, groups):
            # AND all groups for a disqualifying pattern (usually just one group)
            return pl.reduce(
                lambda a, b: a & b,
                [
                    pl.col(col).str.contains(g, literal=False, strict=False)
                    for g in groups
                ],
            )

        def disq_all_patterns_expr(col):
            # OR all disqualifying patterns
            return pl.reduce(
                lambda a, b: a | b,
                [disq_pattern_expr(col, groups) for groups in disqualifying_terms],
            )

        disq_filter = pl.reduce(
            lambda a, b: a | b, [disq_all_patterns_expr(col) for col in search_columns]
        )
        records_before = len(search_df)
        search_df = search_df.filter(~disq_filter)
        records_after = len(search_df)
        filtered_count = records_before - records_after
        logger.info(
            f"Filtered out {filtered_count} records with disqualifying terms ({records_after} remaining)"
        )
        if search_df.is_empty():
            logger.warning("No records remaining after disqualifying terms filter")
            return pl.DataFrame()

    if search_df.is_empty():
        logger.error("No matching records found in search stage")
        return pl.DataFrame()

    logger.info(f"Found {len(search_df)} matching records")

    # Extraction: use proximity logic for each concept
    if extract_matches in ["primary", "both", True]:
        logger.info("Using proximity patterns for extraction on filtered records")
        extraction_exprs = []
        for concept, patterns in queries_for_search.items():
            prox_regex = concept_patterns_to_regex(patterns, proximity=300)
            for col in search_columns:
                extraction_exprs.append(
                    pl.col(col)
                    .str.extract_all(prox_regex)
                    .alias(f"{concept}_extracted_from_{col}")
                )
        search_df = search_df.with_columns(extraction_exprs)

    # Add group counts for primary queries if requested
    if add_group_counts in ["primary", "both"]:
        logger.info("Adding group counts for primary queries")
        group_count_exprs = []
        for concept, patterns in queries_for_search.items():
            for i, pattern_group in enumerate(patterns):
                group_name = f"{concept}_group_{i + 1}_count"
                # Count matches for this specific pattern group across all search columns
                col_matches = []
                for col in search_columns:
                    group_expr = pl.reduce(
                        lambda a, b: a & b,
                        [
                            pl.col(col).str.contains(
                                f"(?i){g}", literal=False, strict=False
                            )
                            for g in pattern_group
                        ],
                    )
                    col_matches.append(group_expr.cast(pl.Int32))

                # Sum across all search columns for this group
                total_group_matches = pl.reduce(lambda a, b: a + b, col_matches)
                group_count_exprs.append(total_group_matches.alias(group_name))

        if group_count_exprs:
            search_df = search_df.with_columns(group_count_exprs)

    # Process secondary queries if provided
    if secondary_queries and extract_matches in ["secondary", "both"]:
        logger.info(
            f"Processing {len(secondary_queries)} secondary query concepts on filtered records"
        )

        # Use secondary search columns if provided, otherwise use primary search columns
        secondary_cols = (
            secondary_search_columns if secondary_search_columns else search_columns
        )
        logger.info(f"Secondary queries will search in columns: {secondary_cols}")

        # Create search expressions for secondary queries
        secondary_queries_for_search = {
            k: v for k, v in secondary_queries.items() if k != "disqualifying_terms"
        }

        # Apply secondary search filter to already filtered data
        if secondary_queries_for_search:

            def secondary_pattern_group_expr(col, groups):
                return pl.reduce(
                    lambda a, b: a & b,
                    [
                        pl.col(col).str.contains(
                            f"(?i){g}", literal=False, strict=False
                        )
                        for g in groups
                    ],
                )

            def secondary_concept_expr(col, patterns):
                return pl.reduce(
                    lambda a, b: a | b,
                    [secondary_pattern_group_expr(col, groups) for groups in patterns],
                )

            def secondary_all_concepts_expr(col):
                return pl.reduce(
                    lambda a, b: a | b,
                    [
                        secondary_concept_expr(col, patterns)
                        for patterns in secondary_queries_for_search.values()
                    ],
                )

            secondary_filter_expr = pl.reduce(
                lambda a, b: a | b,
                [secondary_all_concepts_expr(col) for col in secondary_cols],
            )

            # Filter records that match secondary queries
            records_before_secondary = len(search_df)
            search_df = search_df.filter(secondary_filter_expr)
            records_after_secondary = len(search_df)
            logger.info(
                f"Secondary filter: {records_before_secondary} -> {records_after_secondary} records"
            )

            if search_df.is_empty():
                logger.warning("No records remaining after secondary queries filter")
                return pl.DataFrame()

        # Extract secondary query patterns
        if extract_matches in ["secondary", "both"]:
            logger.info("Extracting secondary query patterns")
            secondary_extraction_exprs = []
            for concept, patterns in secondary_queries_for_search.items():
                prox_regex = concept_patterns_to_regex(patterns, proximity=300)
                for col in secondary_cols:
                    secondary_extraction_exprs.append(
                        pl.col(col)
                        .str.extract_all(prox_regex)
                        .alias(f"secondary_{concept}_extracted_from_{col}")
                    )
            search_df = search_df.with_columns(secondary_extraction_exprs)

        # Add group counts for secondary queries if requested
        if add_group_counts in ["secondary", "both"]:
            logger.info("Adding group counts for secondary queries")
            secondary_group_count_exprs = []
            for concept, patterns in secondary_queries_for_search.items():
                for i, pattern_group in enumerate(patterns):
                    group_name = f"secondary_{concept}_group_{i + 1}_count"
                    # Count matches for this specific pattern group across secondary search columns
                    col_matches = []
                    for col in secondary_cols:
                        group_expr = pl.reduce(
                            lambda a, b: a & b,
                            [
                                pl.col(col).str.contains(
                                    f"(?i){g}", literal=False, strict=False
                                )
                                for g in pattern_group
                            ],
                        )
                        col_matches.append(group_expr.cast(pl.Int32))

                    # Sum across all secondary search columns for this group
                    total_group_matches = pl.reduce(lambda a, b: a + b, col_matches)
                    secondary_group_count_exprs.append(
                        total_group_matches.alias(group_name)
                    )

            if secondary_group_count_exprs:
                search_df = search_df.with_columns(secondary_group_count_exprs)

    # Handle identifier and coordinate patterns using the Polars-compatible regex logic
    if identifier_patterns:
        logger.info("Applying extraction of identifiers from matched records")
        identifier_extract_exprs = create_extraction_expressions(
            identifier_patterns,
            ["full_text"],  # search_columns
        )
        search_df = search_df.with_columns(identifier_extract_exprs)
        accession_cols = [
            col
            for col in search_df.columns
            if col.startswith(
                tuple(
                    ["genbank", "refseq", "uniprot", "general_accessions", "assembly"]
                )
            )
        ]
        if accession_cols:
            search_df = search_df.with_columns(
                pl.concat_list(
                    [
                        pl.col(col).list.drop_nulls().list.unique()
                        for col in accession_cols
                    ]
                ).alias("all_accessions")
            ).drop(accession_cols)

    if coordinate_patterns:
        logger.info(
            "Applying extraction expressions of coordinates to matching records"
        )
        coordinate_extract_exprs = create_extraction_expressions(
            coordinate_patterns,
            ["full_text"],  # search_columns
        )
        search_df = search_df.with_columns(coordinate_extract_exprs)
        # Combine all *_coordinates_extracted_from_full_text columns if present
        coord_cols = [
            col
            for col in search_df.columns
            if col.endswith("_coordinates_extracted_from_full_text")
        ]
        if coord_cols:
            search_df = search_df.with_columns(
                pl.concat_list(
                    [pl.col(col).list.drop_nulls().list.unique() for col in coord_cols]
                ).alias("all_coordinates")
            ).drop(coord_cols)

    # clean extraction
    logger.info("dropping unmatched concepts")
    search_df = drop_empty_or_null_columns(search_df)

    # Calculate total matches from extraction columns
    query_keys = [
        key for key, value in primary_queries.items() if key != "disqualifying_terms"
    ]
    if secondary_queries:
        secondary_keys = [
            f"secondary_{key}"
            for key, value in secondary_queries.items()
            if key != "disqualifying_terms"
        ]
        query_keys.extend(secondary_keys)

    # Find extraction columns that match our query concepts
    extraction_cols = [col for col in search_df.columns if "_extracted_from_" in col]
    query_extraction_cols = []
    for col in extraction_cols:
        concept_name = col.split("_extracted_from_")[0]
        if any(concept_name.startswith(key) for key in query_keys):
            query_extraction_cols.append(col)

    if query_extraction_cols:
        search_df = search_df.with_columns(
            pl.sum_horizontal(
                [pl.col(col).list.n_unique() for col in query_extraction_cols]
            ).alias("total_matches")
        ).sort(by="total_matches", descending=True)
    else:
        # Fallback: use group count columns if no extraction columns
        count_cols = [col for col in search_df.columns if col.endswith("_count")]
        if count_cols:
            search_df = search_df.with_columns(
                pl.sum_horizontal([pl.col(col) for col in count_cols]).alias(
                    "total_matches"
                )
            ).sort(by="total_matches", descending=True)
        else:
            # Final fallback: add a default total_matches column
            search_df = search_df.with_columns(pl.lit(1).alias("total_matches"))

    return search_df


def load_simple_patterns(
    file_path: str, concept_name: str
) -> Dict[str, List[List[str]]]:
    """Load patterns from a simple text file (one pattern per line) and convert to query format.

    Args:
        file_path: Path to text file with one pattern per line
        concept_name: Name for the concept group (derived from file stem)

    Returns:
        Dictionary in the format expected by process_literature_lazy
    """
    patterns = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):  # Skip empty lines and comments
                patterns.append(
                    [line]
                )  # Each pattern is wrapped in a list for group format

    if not patterns:
        raise ValueError(f"No valid patterns found in {file_path}")

    return {concept_name: patterns}


def generate_summary(df: pl.DataFrame) -> Dict[str, int]:
    """Generate processing summary statistics."""
    if df.is_empty():
        return {"total_records": 0}

    summary = {
        "total_records": len(df),
    }

    # Check if total_extractions column exists
    if "total_matches" in df.columns:
        summary["records_with_extractions"] = len(
            df.filter(pl.col("total_matches") > 0)
        )
    else:
        # Fallback: count records with any extraction
        extract_cols = [col for col in df.columns if "_extracted_from_" in col]
        if extract_cols:
            # Create a condition that checks if any extraction column has non-empty lists
            extraction_conditions = [pl.col(col).list.len() > 0 for col in extract_cols]
            any_extraction = pl.fold(
                False, lambda acc, x: acc | x, extraction_conditions
            )
            summary["records_with_extractions"] = len(df.filter(any_extraction))
        else:
            summary["records_with_extractions"] = 0

    # Add extraction type counts
    extract_cols = [col for col in df.columns if "_extracted_from_" in col]
    for col in extract_cols:
        pattern_type = col.split("_extracted_from_")[0]
        count = len(df.filter(pl.col(col).list.len() > 0))
        summary[f"{pattern_type}_matches"] = count

    # Add idnetifier and cooridnate match counts
    match_cols = [col for col in df.columns if col.startswith("all_")]
    for col in match_cols:
        query_name = col.replace("all_", "")
        try:
            count = len(df.filter(pl.col(col).list.len() > 0))
            summary[f"{query_name}_query_matches"] = count
        except:  # noqa
            print(type(df[col]))

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Unified Literature Processing Pipeline with Polars Lazy Evaluation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required arguments
    parser.add_argument(
        "--parquet-pattern",
        type=str,
        required=True,
        help="Glob pattern for parquet files (e.g., 'data/pubmed_central/parquet_files/*/*.parquet')",
    )
    parser.add_argument(
        "--queries-file",
        type=str,
        help="JSON file containing search queries (required unless using --simple-mode)",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Output path for processed data (without extension)",
    )
    parser.add_argument(
        "--search-columns",
        type=str,
        default="title,abstract_text,full_text",
        help="Columns to search for patterns",
    )

    # Optional arguments
    parser.add_argument(
        "--identifier-patterns-file",
        type=str,
        default="data/assets/patterns/polars/identifiers.json",
        help="JSON file containing identifier patterns",
    )
    parser.add_argument(
        "--coordinate-patterns-file",
        type=str,
        default="data/assets/patterns/polars/coordinates.json",
        help="JSON file containing coordinate patterns",
    )
    parser.add_argument(
        "--extract-matches",
        type=str,
        default="primary",
        choices=["primary", "secondary", "both", "none"],
        help="Which queries to extract matches for: primary, secondary, both, or none",
    )
    parser.add_argument(
        "--secondary-queries-file",
        type=str,
        help="JSON file containing secondary search queries to apply after primary filtering",
    )
    parser.add_argument(
        "--secondary-search-columns",
        type=str,
        help="Columns to search for secondary patterns (comma-separated). If not provided, uses same columns as primary search",
    )
    parser.add_argument(
        "--add-group-counts",
        type=str,
        choices=["primary", "secondary", "both"],
        help="Add columns counting matches for each pattern group: primary, secondary, or both",
    )
    parser.add_argument(
        "--min-queries-per-match",
        type=int,
        default=1,
        help="Filter the results to only keep those that are matched by at least this many different query groups",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Path to log file for detailed logs",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--simple-mode",
        type=str,
        help="Simple mode: provide a text file with one pattern per line instead of JSON queries",
    )
    args = parser.parse_args()
    logger = setup_logging(verbose=args.verbose, log_file=args.log_file)

    # Validate arguments
    if not args.simple_mode and not args.queries_file:
        parser.error("Either --queries-file or --simple-mode must be provided")
    if args.simple_mode and args.queries_file:
        parser.error(
            "Cannot use both --queries-file and --simple-mode at the same time"
        )

    # Load queries based on mode
    if args.simple_mode:
        # Simple mode: load patterns from text file
        simple_file_path = Path(args.simple_mode)
        if not simple_file_path.exists():
            parser.error(f"Simple mode file not found: {args.simple_mode}")

        # Create concept name from file stem
        concept_name = simple_file_path.stem
        logger.info(
            f"Loading simple patterns from {args.simple_mode} as concept '{concept_name}'"
        )

        queries = load_simple_patterns(args.simple_mode, concept_name)
        logger.info(
            f"Loaded {len(queries[concept_name])} patterns for concept '{concept_name}'"
        )
    else:
        # Standard mode: load from JSON file
        queries = json.loads(open(args.queries_file, "r", encoding="utf-8").read())

    secondary_queries = None
    secondary_search_columns = None
    if args.secondary_queries_file:
        secondary_queries = json.loads(
            open(args.secondary_queries_file, "r", encoding="utf-8").read()
        )
        logger.info(f"Loaded {len(secondary_queries)} secondary query concepts")

        if args.secondary_search_columns:
            secondary_search_columns = args.secondary_search_columns.split(",")
            logger.info(
                f"Secondary queries will search in columns: {secondary_search_columns}"
            )
        else:
            logger.info(
                "Secondary queries will use same search columns as primary queries"
            )

    identifier_patterns = None
    if args.identifier_patterns_file:
        identifier_patterns = json.loads(
            open(args.identifier_patterns_file, "r", encoding="utf-8").read()
        )

    coordinate_patterns = None
    if args.coordinate_patterns_file:
        coordinate_patterns = json.loads(
            open(args.coordinate_patterns_file, "r", encoding="utf-8").read()
        )

    # Process literature using lazy evaluation
    logger.info("Processing literature with lazy evaluation...")

    processed_df = process_literature_lazy(
        parquet_pattern=args.parquet_pattern,
        primary_queries=queries,
        secondary_queries=secondary_queries,
        secondary_search_columns=secondary_search_columns,
        extract_matches=args.extract_matches,
        identifier_patterns=identifier_patterns,
        coordinate_patterns=coordinate_patterns,
        search_columns=args.search_columns.split(","),
        add_group_counts=args.add_group_counts,
        logger=logger,
    )

    if processed_df.is_empty():
        logger.warning("⚠️ No matching records found")
        return

    # Add number of matches info
    # Apply number of matches filter if given
    if args.min_queries_per_match > 1:
        logger.info(
            f"Applying number of matches filter: {args.min_queries_per_match} matches"
        )
        logger.info(f"dataframe height before: {processed_df.height}")
        processed_df = processed_df.filter(
            pl.col("total_matches") > args.min_queries_per_match
        )
        logger.info(f"dataframe height after: {processed_df.height}")

    # Flatten extraction results
    logger.info("Flattening extraction results for csv export...")
    droppers = set(["full_text", "abstract_text", "pmid", "file_path"]).intersection(
        processed_df.columns
    )
    flattened_df = convert_nested_cols(
        drop_empty_or_null_columns(processed_df.drop(droppers)), separator=","
    )
    # Log some information about the results
    logger.info(f"Processed dataframe shape: {processed_df.shape}")
    logger.info(f"Flattened dataframe shape: {flattened_df.shape}")
    logger.debug(f"Available columns: {flattened_df.columns}")

    # Show some sample extractions if available
    extract_cols = [col for col in processed_df.columns if "_extracted_from_" in col]
    if extract_cols:
        logger.debug(f"Extraction columns found: {extract_cols}")
        for col in extract_cols:
            non_empty_count = len(processed_df.filter(pl.col(col).list.len() > 0))
            logger.info(f"  {col}: {non_empty_count} records with extractions")
            if non_empty_count > 0:
                logger.info(
                    f"Example: {processed_df.filter(pl.col(col).list.len() > 0).sample(1)[col].to_list()}"
                )

    else:
        logger.warning("No extraction columns found")

    # Save results
    logger.info("Saving results...")
    output_path = Path(args.output_path)

    output_path.mkdir(parents=True, exist_ok=True)
    # Save as both parquet and csv
    flattened_df.write_csv(f"{output_path}/flattened.csv")
    logger.info(f"Flattened dataframe saved to {output_path}/flattened.csv")
    processed_df.write_parquet(f"{output_path}/prcoessed.parquet")
    logger.info(f"raw dataframe saved to {output_path}/prcoessed.parquet")

    # Generate and display summary
    logger.info("Generating summary...")
    summary = generate_summary(processed_df)

    # Display summary
    Panel("📊 Literature Processing Summary", style="bold green", expand=False)
    table = Table(title="Processing Results", expand=True)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta", no_wrap=True)

    for key, value in summary.items():
        table.add_row(key.replace("_", " ").title(), str(value))
    from rich.console import Console

    Console().print(table)


if __name__ == "__main__":
    main()
