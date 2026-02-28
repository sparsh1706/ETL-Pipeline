#!/usr/bin/env bash
# ------------------------------------------------------------
# run_end_to_end_pipeline.sh
#
# End-to-end data pipeline script:
# 1. Runs Spark transformations (Extract + Transform)
# 2. Loads transformed data into DuckDB (Load)
#
# Design principles:
# - Atomic execution (all-or-nothing)
# - Clear failure handling with rollback
# - Readable, reproducible, and documented
# ------------------------------------------------------------

set -o pipefail

# -----------------------------
# Tool configuration
# -----------------------------
DUCKDB_BIN="duckdb"
SPARK_SUBMIT_BIN="spark-submit"

# -----------------------------
# Path configuration
# -----------------------------
SPARK_JOB_PATH="/home/sparshsharma/fire_pollution_analysis.py"
DUCKDB_DATABASE_PATH="/home/sparshsharma/final.db"
OUTPUT_DIR="/home/sparshsharma/output"
DUCKDB_SQL_PATH="/home/sparshsharma/duckdb_analytical_queries.sql"

# DuckDB placeholder replacement value
PARQUET_LOAD_PATH="${OUTPUT_DIR}/*.parquet"

# -----------------------------
# Utility functions
# -----------------------------

print_message() {
    printf "%50s\n" | tr " " "-"
    printf "%s\n" "$1"
    printf "%50s\n" | tr " " "-"
}

rollback() {
    # Ensures atomicity by cleaning up partial outputs
    rm -rf "$OUTPUT_DIR"
    rm -f "$DUCKDB_DATABASE_PATH"
}

check_status() {
    local success_msg="$1"
    local failure_msg="$2"

    if [ $? -eq 0 ]; then
        print_message "$success_msg"
    else
        print_message "$failure_msg"
        rollback
        exit 1
    fi
}

# -----------------------------
# Pipeline steps
# -----------------------------

run_spark_transformations() {
    # Remove previous outputs to ensure clean state
    rm -rf "$OUTPUT_DIR"

    "$SPARK_SUBMIT_BIN" \
        --master local[*] \
        "$SPARK_JOB_PATH"

    check_status \
        "Spark job completed successfully (Extract & Transform)." \
        "Spark job FAILED."
}

run_duckdb_load() {
    # Replace LOADPATH placeholder in SQL and execute in DuckDB
    sed "s|\$LOADPATH|${PARQUET_LOAD_PATH//\//\\/}|g" "$DUCKDB_SQL_PATH" \
        | "$DUCKDB_BIN" "$DUCKDB_DATABASE_PATH"

    check_status \
        "Data successfully loaded into DuckDB (Load)." \
        "DuckDB load FAILED."
}

# -----------------------------
# Pipeline execution
# -----------------------------

print_message "STARTING END-TO-END DATA PIPELINE"

run_spark_transformations
run_duckdb_load

check_status "PIPELINE COMPLETED SUCCESSFULLY" "PIPELINE FAILED"
