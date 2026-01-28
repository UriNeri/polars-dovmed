"""
CLI interface for polars-dovmed package.

Simple command router that delegates to the appropriate script.
"""

import argparse
import sys


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="dovmed",
        description="Literature mining tools for PubMed Central Open Access subset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available commands:
  scan           Scan literature for patterns and extract matches
  download       Download PMC OA data subsets  
  build-parquet  Convert PMC tar.gz files to parquet format
  create-patterns Generate query patterns using LLM

Use 'dovmed COMMAND --help' for command-specific help.

Examples:
  dovmed scan --parquet-pattern "data/pubmed_central/parquet_files/*/*.parquet" \\
              --queries-file queries.json --output-path results/

  dovmed download oa_comm oa_other --output-dir data/pubmed_central

  dovmed build-parquet --pmc-oa-dir data/pubmed_central/pmc_oa/ \\
                       --parquet-dir data/pubmed_central/parquet_files/

  dovmed create-patterns --input-text "RNA viruses" \\
                         --output-file queries.json \\
                         --model "gpt-4" --api-key $API_KEY
        """,
    )

    parser.add_argument(
        "command",
        choices=["scan", "download", "build-parquet", "create-patterns"],
        help="Command to run",
    )

    parser.add_argument(
        "args", nargs=argparse.REMAINDER, help="Arguments to pass to the command"
    )

    args = parser.parse_args()

    # Route to the appropriate script
    try:
        if args.command == "scan":
            from polars_dovmed.scan_pmc import main as scan_main

            # Replace sys.argv with the command args
            sys.argv = ["scan_pmc.py"] + args.args
            scan_main()

        elif args.command == "download":
            from polars_dovmed.get_data.download import main as download_main

            sys.argv = ["download.py"] + args.args
            download_main()

        elif args.command == "build-parquet":
            from polars_dovmed.convert_pmctargz_parquet import main as convert_main

            sys.argv = ["convert_pmctargz_parquet.py"] + args.args
            convert_main()

        elif args.command == "create-patterns":
            from polars_dovmed.llm_create_query_patterns import main as patterns_main

            sys.argv = ["llm_create_query_patterns.py"] + args.args
            patterns_main()

        else:
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
