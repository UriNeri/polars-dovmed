#!/bin/bash

# Test command for simple mode using test_simple_patterns.txt
# This tests the new --simple-mode functionality on a subset of parquet files

python src/polars_dovmed/scan_pmc.py \
    --parquet-pattern "data/pubmed_central/parquet_files/worker_00/batch_000[1-3].parquet" \
    --simple-mode test_simple_patterns.txt \
    --output-path results/simple_mode_test \
    --search-columns "title,abstract_text,full_text" \
    --extract-matches primary \
    --add-group-counts primary \
    --verbose

echo "Test completed. Check results/simple_mode_test/ for output files."
echo "The concept group will be named 'test_simple_patterns' (from the filename stem)."