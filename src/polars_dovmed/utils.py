"""
Utility functions for the literature analysis pipeline using local PMC OA collection.
Many functions here assume the pmc_oa collection was fetched and is available. see get_data/ folder
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn

load_dotenv()  # load env vars from .env file for api key and email.

# Initialize module-level logger - SHOULD inherit configuration from root logger
logger = logging.getLogger(__name__)

# Initialize console for rich output - auto-detect terminal capabilities (important messes up slurrm stdout/err)
console = Console(
    width=None,
    force_terminal=sys.stdout.isatty(),  # Only use rich formatting if we're in a real terminal
    legacy_windows=False,
    no_color=not sys.stdout.isatty(),  # Disable colors if output is redirected
)


# Configure rich logging
def setup_logging(verbose: bool = False, log_file: Optional[str] = None):
    """Set up rich logging with appropriate level and optional file output"""
    level = logging.DEBUG if verbose else logging.INFO

    # Use simple logging for non-interactive environments (like SLURM)
    handlers: List[logging.Handler] = []
    if sys.stdout.isatty():
        # Interactive terminal - use rich formatting
        handlers = [
            RichHandler(
                console=console, rich_tracebacks=True, show_path=True, show_time=True
            )
        ]
    else:
        # Non-interactive (redirected output) - use simple formatter
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        stream_handler.setFormatter(formatter)
        handlers = [stream_handler]

    # Add file handler if log_file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(level)

        # Create a formatter for file output (without rich formatting)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)  # type: ignore

    logging.basicConfig(
        level=level, format="%(message)s", datefmt="[%X]", handlers=handlers
    )

    # Suppress some noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


class ChunkProgressReporter:
    """
    Progress reporter that works safely with logging and SLURM jobs.

    - Interactive mode: Uses Rich progress bar
    - Non-interactive mode: Uses logging with configurable intervals
    - SLURM-safe: Doesn't create excessive output in batch jobs
    """

    def __init__(
        self,
        total_chunks: int,
        description: str = "Processing chunks",
        logger: logging.Logger = logging.getLogger(__name__),
        log_interval: int = 5,
    ):
        """
        Initialize progress reporter.

        Args:
            total_chunks: Total number of chunks to process
            description: Description for progress display
            logger: Logger instance for non-interactive progress
            log_interval: Log progress every N chunks (for SLURM/non-interactive)
        """
        self.total_chunks = total_chunks
        self.description = description
        self.logger = logger
        self.log_interval = log_interval
        self.current_chunk = 0
        self.is_interactive = sys.stdout.isatty()

        # Initialize progress tracking
        if self.is_interactive:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                "[progress.percentage]{task.percentage:>3.0f}%",
                console=console,
                expand=True,
            )
            self.progress.start()
            self.task = self.progress.add_task(description, total=total_chunks)
        else:
            self.progress = None
            self.task = None
            self.logger.info(
                f"🚀 {description}: Starting processing of {total_chunks} chunks"
            )

    def update(self, advance: int = 1, chunk_info: str = "", description: str = ""):
        """
        Update progress by advancing N chunks.

        Args:
            advance: Number of chunks to advance
            chunk_info: Optional additional info about current chunk
        """
        self.current_chunk += advance

        if self.is_interactive:
            # Interactive mode: update rich progress bar
            if self.progress and self.task is not None:
                self.progress.update(
                    self.task, advance=advance, description=description
                )
        else:
            # Non-interactive mode: log at intervals
            if (
                self.current_chunk % self.log_interval == 0
                or self.current_chunk == self.total_chunks
            ):
                percentage = (self.current_chunk / self.total_chunks) * 100
                chunk_desc = f" - {chunk_info}" if chunk_info else ""
                self.logger.info(
                    f"📊 {self.description}: {self.current_chunk}/{self.total_chunks} chunks "
                    f"({percentage:.1f}%){chunk_desc}"
                )

    def finish(self, success: bool = True):
        """
        Finish progress reporting.

        Args:
            success: Whether processing completed successfully
        """
        if self.is_interactive:
            if self.progress:
                self.progress.stop()
        else:
            status = "✅ Completed" if success else "❌ Failed"
            self.logger.info(
                f"{status} {self.description}: {self.current_chunk}/{self.total_chunks} chunks processed"
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        self.finish(success=success)


def drop_empty_or_null_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Drop columns that are completely null or empty"""
    columns_to_drop = []
    for col in df.columns:
        series = df.get_column(col)
        if series.dtype == pl.List:
            if series.is_null().all() or (series.list.len() == 0).all():
                columns_to_drop.append(col)
        elif series.is_null().all():
            columns_to_drop.append(col)
        elif series.dtype == pl.String:
            if all(series.str.strip_chars() == ""):
                columns_to_drop.append(col)

    if columns_to_drop:
        df = df.drop(columns_to_drop)
    return df


def flatten_struct(
    df: Union[pl.DataFrame, pl.LazyFrame],
    struct_columns: Union[str, List[str]],
    separator: str = ":",
    drop_original_struct: bool = True,
    recursive: bool = False,
    limit: Optional[int] = None,
) -> pl.DataFrame:
    """
    Takes a PolarsFrame and flattens specified struct columns into
    separate columns using a specified separator,
    with options to control recursion and limit the number
    of flattening levels.

    :param df: A PolarsFrame, either a LazyFrame or DataFrame.
    :type df: PolarsFrame
    :param struct_columns: The column or columns in the PolarsFrame that contain struct data.
    This function is designed to flatten the struct data into separate columns based on the fields within the struct.
    :type struct_columns: Union[str, List[str]]
    :param separator: Specifies the character or string that will be used to separate the original
    column name from the nested field names when flattening a nested struct column.
    :type separator: str (optional)
    :param drop_original_struct: Determines whether the original struct columns should be dropped after flattening or not,
    defaults to True.
    :type drop_original_struct: bool (optional)
    :param recursive: Determines whether the flattening process should be applied recursively to
    all levels of nested structures within the specified struct columns, defaults to False.
    :type recursive: bool (optional)
    :param limit: Determines the maximum number of levels to flatten the struct columns.
    If `limit` is set to a positive integer, the function will flatten the struct columns up to that specified level.
    If `limit` is set to `None`, there is no limit.
    :type limit: int
    :return: returns a pl.DataFrame.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    ldf = df.lazy()
    if isinstance(struct_columns, str):
        struct_columns = [struct_columns]
    if not recursive:
        limit = 1
    if limit is not None and not isinstance(limit, int):
        raise ValueError("limit must be a positive integer or None")
    if limit is not None and limit < 0:
        raise ValueError("limit must be a positive integer or None")
    if limit == 0:
        print("limit of 0 will result in no transformations")
        return df
    ldf = df.lazy()  # noop if df is LazyFrame
    all_column_names = ldf.collect_schema().names()
    if any(separator in (witness := column) for column in all_column_names):
        print(
            f'separator "{separator}" found in column names, e.g. "{witness}". '
            "If columns would be repeated, this function will error"
        )
    non_struct_columns = list(set(ldf.collect_schema().names()) - set(struct_columns))
    struct_schema = ldf.select(*struct_columns).collect_schema()
    col_dtype_expr_names = [(struct_schema[c], pl.col(c), c) for c in struct_columns]
    result_names: Dict[str, pl.Expr] = {}
    level = 0
    while (limit is None and col_dtype_expr_names) or (
        limit is not None and level < limit
    ):
        level += 1
        new_col_dtype_exprs = []
        for dtype, col_expr, name in col_dtype_expr_names:
            if not isinstance(dtype, pl.Struct):
                if name in result_names:
                    raise ValueError(
                        f"Column name {name} would be created at least twice after flatten_struct"
                    )
                result_names[name] = col_expr
                continue
            if any(separator in (witness := field.name) for field in dtype.fields):
                print(
                    f'separator "{separator}" found in field names, e.g. "{witness}" in {name}. '
                    "If columns would be repeated, this function will error"
                )
            new_col_dtype_exprs += [
                (
                    field.dtype,
                    col_expr.struct.field(field.name),
                    name + separator + field.name,
                )
                for field in dtype.fields
            ]
            if not drop_original_struct:
                ldf = ldf.with_columns(
                    col_expr.struct.field(field.name).alias(
                        name + separator + field.name
                    )
                    for field in dtype.fields
                )
        col_dtype_expr_names = new_col_dtype_exprs
    if drop_original_struct and level == limit and col_dtype_expr_names:
        for _, col_expr, name in col_dtype_expr_names:
            result_names[name] = col_expr
    if any((witness := column) in non_struct_columns for column in result_names):
        raise ValueError(
            f"Column name {witness} would be created after flatten_struct, but it's already a non-struct column"
        )
    if drop_original_struct:
        ldf = ldf.select(
            [pl.col(c) for c in non_struct_columns]
            + [col_expr.alias(name) for name, col_expr in result_names.items()]
        )

    return ldf.collect()


def flatten_all_structs(
    df: Union[pl.DataFrame, pl.LazyFrame],
    separator: str = ",",
    drop_original_struct: bool = True,
    recursive: bool = True,
    limit: Optional[int] = None,
) -> pl.DataFrame:
    """Flatten all struct columns in a dataframe"""
    struct_cols = [col for col, dtype in df.schema.items() if dtype == pl.Struct]
    return flatten_struct(
        df,
        struct_cols,
        separator=separator,
        drop_original_struct=drop_original_struct,
        recursive=recursive,
        limit=limit,
    )


def convert_nested_cols(
    df: Union[pl.DataFrame, pl.LazyFrame],
    separator: str = ",",
    drop_original_struct: bool = True,
    recursive: bool = True,
    limit: Optional[int] = None,
) -> pl.DataFrame:
    """Converts nested columns  by the dtype. Structs are flattened, while lists, arrays and objects are converted to strings."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    list_cols = [col for col, dtype in df.schema.items() if dtype == pl.List]
    # print(f"list_cols: {list_cols}")
    array_cols = [col for col, dtype in df.schema.items() if dtype == pl.Array]
    # print(f"array_cols: {array_cols}")
    object_cols = [col for col, dtype in df.schema.items() if dtype == pl.Object]
    # print(f"object_cols: {object_cols}")
    struct_cols = [col for col, dtype in df.schema.items() if dtype == pl.Struct]
    # print(f"struct_cols: {struct_cols}")
    for col in struct_cols:
        df = flatten_struct(
            df,
            col,
            separator=separator,
            drop_original_struct=drop_original_struct,
            recursive=recursive,
            limit=limit,
        )
    for col in set(list_cols + array_cols + object_cols):
        # Convert list elements to strings and join them
        df = df.with_columns(
            pl.col(col)
            .list.eval(pl.element().cast(pl.Utf8, strict=False))
            .list.join(separator)
            .alias(col)
        )
    return df


def clean_and_normalize_dataframe(
    df: pl.DataFrame, logger: logging.Logger = logging.getLogger(__name__)
) -> pl.DataFrame:
    """Clean and normalize the combined dataframe"""
    logger.info("Cleaning and normalizing dataframe...")
    logger.debug(f"Input shape: {df.shape}")
    logger.debug(f"Input columns: {df.columns}")

    # Remove columns that are completely null or empty
    columns_to_drop = []
    for col in df.columns:
        if (
            df[col].is_null().all()
            or (df[col].cast(pl.String).str.strip_chars() == "").all()
        ):
            columns_to_drop.append(col)

    if columns_to_drop:
        logger.info(f"Dropping {len(columns_to_drop)} empty columns: {columns_to_drop}")
        df = df.drop(columns_to_drop)

    # Handle duplicate columns - prefer the more specific ones
    duplicate_mappings = {
        "journal": "Journal",  # Keep Journal, drop journal
        "License": "PMC_License",  # Keep PMC_License, drop License if PMC_License exists
    }

    columns_to_drop = []
    for old_col, new_col in duplicate_mappings.items():
        if old_col in df.columns and new_col in df.columns:
            # Check if the new column has more data
            old_non_null = df[old_col].is_not_null().sum()
            new_non_null = df[new_col].is_not_null().sum()

            if new_non_null >= old_non_null:
                columns_to_drop.append(old_col)
                logger.info(
                    f"Dropping duplicate column '{old_col}' in favor of '{new_col}'"
                )
            else:
                # Rename old to new and drop new
                df = df.rename({old_col: new_col})
                if new_col in df.columns:
                    columns_to_drop.append(new_col)
                logger.info(
                    f"Renaming '{old_col}' to '{new_col}' and dropping original '{new_col}'"
                )

    if columns_to_drop:
        df = df.drop(columns_to_drop)

    # Normalize PMC_ID field - ensure it starts with PMC
    if "PMC_ID" in df.columns:
        df = df.with_columns(
            [
                pl.when(pl.col("PMC_ID").is_not_null() & (pl.col("PMC_ID") != ""))
                .then(
                    pl.when(pl.col("PMC_ID").str.starts_with("PMC"))
                    .then(pl.col("PMC_ID"))
                    .otherwise(pl.concat_str([pl.lit("PMC"), pl.col("PMC_ID")]))
                )
                .otherwise(pl.col("PMC_ID"))
                .alias("PMC_ID")
            ]
        )

    # Clean up PMC_File_Path - remove any null or empty paths
    if "PMC_File_Path" in df.columns:
        df = df.with_columns(
            [
                pl.when(
                    pl.col("PMC_File_Path").is_null() | (pl.col("PMC_File_Path") == "")
                )
                .then(None)
                .otherwise(pl.col("PMC_File_Path"))
                .alias("PMC_File_Path")
            ]
        )

    # Update has_pmc_file flag based on actual PMC_ID presence
    if "has_pmc_file" in df.columns and "PMC_ID" in df.columns:
        df = df.with_columns(
            [
                pl.when(pl.col("PMC_ID").is_not_null() & (pl.col("PMC_ID") != ""))
                .then(True)
                .otherwise(False)
                .alias("has_pmc_file")
            ]
        )

    logger.info(f"Cleaned dataframe shape: {df.shape}")
    logger.info(f"Final columns: {df.columns}")

    return df


def validate_re_pattern(pattern: str) -> bool:
    """Validate that a regex pattern is compilable with python's re library."""
    try:
        re.compile(pattern)
        return True
    except re.error as e:
        logger.warning(f"Invalid regex pattern '{pattern}': {e}")
        return False


def load_ndjson(
    ndjson_file: str, logger: logging.Logger = logging.getLogger(__name__)
) -> Dict[str, List[str]]:
    """Load patterns from NDJSON (a.k.a JSONL) file - one line per json string, skipping commented lines as in jsonc"""
    patterns_path = Path(ndjson_file)
    patterns = {}
    logger.debug("Opening file for reading")
    with open(patterns_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                # Skip commented lines
                if line.startswith("//"):
                    continue
                try:
                    pattern_data = json.loads(line)
                    pattern_type = pattern_data["pattern_type"]
                    patterns[pattern_type] = pattern_data["patterns"]
                except json.JSONDecodeError:
                    continue
                except KeyError:
                    continue
            else:
                continue  # skipping empty lines
    return patterns


def create_extraction_expressions(
    patterns: Dict[str, List[str]],
    search_columns: List[str] = ["title", "abstract", "full_text"],
    expr_type="extract_all",
) -> List[pl.Expr]:
    """Create polars expressions for extracting patterns (regex) from specified columns.
    see Rust regex [crate](https://docs.rs/regex/latest/regex/)"""
    expressions = []

    for pattern_type, pattern_list in patterns.items():
        # Skip disqualifying_terms as they're handled separately
        if pattern_type == "disqualifying_terms":
            continue

        # Combine all patterns for this type with OR logic
        combined_pattern = "|".join(f"({pattern})" for pattern in pattern_list)

        # Create extraction expressions for each column
        for col in search_columns:
            col_name = f"{pattern_type}_extracted_from_{col}"

            try:
                if expr_type == "contains":
                    extract_expr = (
                        pl.col(col)
                        # .str.to_lowercase()
                        .str.contains(pattern=combined_pattern)
                        # .list.unique()  # Remove duplicates within each extraction
                        # .list.drop_nulls()  # Remove null values
                        .alias(col_name)
                    )
                if expr_type == "extract_all":
                    extract_expr = (
                        pl.col(col)
                        .str.extract_all(combined_pattern)
                        .list.unique()  # Remove duplicates within each extraction
                        .list.drop_nulls()  # Remove null values
                        .alias(col_name)
                    )
                expressions.append(extract_expr)
            except Exception:
                # If column doesn't exist or other error, create an empty list expression
                expressions.append(pl.lit([]).cast(pl.List(pl.String)).alias(col_name))

    return expressions


def normalize_column_name(name: str) -> str:
    """Convert column name to snake_case and remove trailing whitespace"""
    # Remove trailing whitespace
    name = name.strip()
    # to snake_case
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = name.lower()
    # Remove multiple underscores
    name = re.sub(r"_+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")
    return name


def unstruct_with_suffix(
    input_df: pl.DataFrame,
    suffix: str = "_unnested",
    col_name: str = "Devstral-Small-2505",
) -> pl.DataFrame:
    # Get the field names of the struct column
    struct_fields = input_df[col_name].struct.fields
    # print(struct_fields)
    if col_name not in input_df.columns:
        raise ValueError(f"Column '{col_name}' not found in DataFrame.")
    # Unnest the 'data' column and add the suffix to the new column names
    df_unnested = input_df.with_columns(
        pl.col(col_name).struct.rename_fields(
            [f"{field}{suffix}" for field in struct_fields]
        )
    ).unnest(col_name)
    return df_unnested


def clean_pattern_for_polars(pattern: str) -> str:
    """Clean a regex pattern to make it Polars-compatible."""
    # Remove lookaheads and non-capturing groups
    pattern = re.sub(r"\(\?=\.\*[^)]+\)", "", pattern)
    pattern = pattern.replace("(?:", "(")

    # Handle quantifiers
    pattern = re.sub(r"\.{{(\d+),(\d+)}}", r".{\1,\2}", pattern)
    pattern = re.sub(r"\.{{(\d+),}}", r".{\1,}", pattern)
    pattern = re.sub(r"\.{{,(\d+)}}", r".{0,\1}", pattern)
    pattern = re.sub(r"\.{{(\d+)}}", r".{\1}", pattern)

    # Remove regex syntax that might interfere with Polars
    pattern = pattern.replace("\\b", "")  # Remove word boundaries
    pattern = re.sub(
        r"\[\\^a-zA-Z\]", "[^a-zA-Z]", pattern
    )  # Ensure non-letter character checks work

    # Remove any remaining empty groups
    pattern = re.sub(r"\(\)", "", pattern)
    pattern = pattern.strip()

    return pattern if pattern else ".*"  # Fallback to match anything


def pattern_groups_to_regex(groups, join_type="and", proximity=None):
    """
    Convert a list of group strings (each group: OR, groups: AND) to a regex string.
    - join_type: "and" (default) = all groups must match (AND logic)
    - proximity: if set, join groups with .{0,proximity} for proximity matching
    """
    if not groups:
        return ""
    if proximity is not None:
        # Proximity: join groups with .{0,proximity}
        prox = f".{{0,{proximity}}}"
        return prox.join(f"({g})" for g in groups)
    if join_type == "and":
        # AND: use lookahead for each group
        return "".join(f"(?=.*({g}))" for g in groups)
    elif join_type == "or":
        # OR: just join with |
        return "|".join(f"({g})" for g in groups)
    else:
        raise ValueError(f"Unknown join_type: {join_type}")


def concept_patterns_to_regex(patterns, join_type="and", proximity=None):
    """
    Convert a list of patterns (each a list of groups) to a single regex string.
    Patterns are OR'd together.
    """
    regexes = [
        pattern_groups_to_regex(groups, join_type=join_type, proximity=proximity)
        for groups in patterns
    ]
    return "|".join(f"({r})" for r in regexes if r)
