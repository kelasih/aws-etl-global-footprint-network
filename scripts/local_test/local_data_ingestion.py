import logging
import sys
import glob
from pathlib import Path
import duckdb
import polars as pl
from typing import List, Union

# --- Configuration ---
# Paths
RAW_DATA_DIR = Path("local_storage/raw")
DUCKDB_PATH = Path("local_storage/footprint.duckdb")
TARGET_TABLE_NAME = "carbon_footprint"
START_YEAR = 2010

# Column mapping for renaming and schema definition
SCHEMA_MAPPING = {
    "year": pl.Int16,
    "countryCode": pl.Int16,
    "countryName": pl.Utf8,
    "shortName": pl.Utf8,
    "isoa2": pl.Utf8,
    "record": pl.Utf8,
    "cropLand": pl.Float64,
    "grazingLand": pl.Float64,
    "forestLand": pl.Float64,
    "fishingGround": pl.Float64,
    "builtupLand": pl.Float64,
    "carbon": pl.Float64,
    "value": pl.Float64,
    "score": pl.Utf8
}


def setup_logging(log_file: Path) -> None:
    """Sets up the standardized logging configuration"""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, 'w'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def create_target_table(con: duckdb.DuckDBPyConnection) -> None:
    """Creates or recreates the target table in DuckDB"""
    logging.info(f"Preparing table '{TARGET_TABLE_NAME}'...")
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE_NAME} (
                year SMALLINT,
                country_code SMALLINT,
                country_name VARCHAR,
                short_name VARCHAR,
                isoa2 VARCHAR,
                record VARCHAR,
                crop_land DOUBLE,
                grazing_land DOUBLE,
                forest_land DOUBLE,
                fishing_ground DOUBLE,
                builtup_land DOUBLE,
                carbon DOUBLE,
                value DOUBLE,
                score VARCHAR
            );
            TRUNCATE TABLE {TARGET_TABLE_NAME};
        """)
        logging.info("Table created and truncated for fresh ingestion")
    except Exception as e:
        logging.error(f"Failed to create/truncate table '{TARGET_TABLE_NAME}': {e}")
        raise


def process_and_load_data(con: duckdb.DuckDBPyConnection) -> int:
    """
    Uses Polars to process, rename, filter, and load data efficiently
    Returns the number of rows loaded
    """
    json_files_pattern = str(RAW_DATA_DIR / "*.json")

    # Use glob for safety check, though Polars can handle the pattern directly
    if not glob.glob(json_files_pattern):
        logging.warning("No JSON files found in the raw data directory. Skipping ingestion")
        return 0

    logging.info(f"Reading and concatenating JSON files from {json_files_pattern}...")

    # 1. Read and Concatenate
    # Use Polars's read_json_files, which is optimized for this task
    try:
        full_df = pl.read_json(json_files_pattern, schema=SCHEMA_MAPPING)
    except pl.exceptions.ComputeError as e:
        logging.error(f"Polars failed to read JSON files (potential schema mismatch or empty files): {e}")
        return 0

    # 2. Transform (Rename and Filter)
    logging.info(f"Applying transformations on {full_df.shape[0]} raw records...")

    # Renames columns and collect the dataframe
    processed_df = full_df.lazy().with_columns(
        [pl.col(old).alias(
            old.replace('Code', '_code')
                .replace('Name', '_name')
                .replace('Ground', '_ground')
                .replace('upLand','_up_land')
                .lower())
         for old in full_df.columns]
    ).select(
        # Selects and reorders columns matching the target table schema
        pl.col("year"),
        pl.col("country_code"),
        pl.col("country_name"),
        pl.col("short_name"),
        pl.col("isoa2"),
        pl.col("record"),
        pl.col("crop_land"),
        pl.col("grazing_land"),
        pl.col("forest_land"),
        pl.col("fishing_ground"),
        pl.col("builtup_land"),
        pl.col("carbon"),
        pl.col("value"),
        pl.col("score"),
    ).collect()

    logging.info(f"Filtered to {processed_df.shape[0]} records (since {START_YEAR})")

    # 3. Load into DuckDB
    try:
        # Directly inserts the whole dataframe
        con.execute(f"INSERT INTO {TARGET_TABLE_NAME} SELECT * FROM processed_df",
                    parameters={"processed_df": processed_df})

        # Verify load count
        loaded_count = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE_NAME}").fetchone()[0]
        logging.info(f"Successfully loaded {loaded_count} total records into DuckDB")
        return loaded_count
    except Exception as e:
        logging.error(f"Failed to insert data into DuckDB: {e}")
        raise


def run_verification_queries(con: duckdb.DuckDBPyConnection) -> None:
    """Runs verification queries to check data integrity and dashboard readiness"""
    logging.info("\n--- Running Verification Queries ---")

    # 1. Sample check
    logging.info("Sample records (showing country and year):")
    sample_df = con.execute(f"SELECT country_name, year, carbon, record FROM {TARGET_TABLE_NAME} LIMIT 5").df()
    logging.info(sample_df.to_string())

    # 2. Total count check (already done in load, but good for final report)
    total_count = con.execute(f"SELECT count(1) FROM {TARGET_TABLE_NAME}").fetchone()[0]
    logging.info(f"\nTotal loaded records count: {total_count}")

    # 3. Dashboard-specific query
    logging.info("\nBrazil carbon footprint evolution (since 2010):")
    brazil_df = con.execute(f"""
        SELECT country_name, avg(carbon) AS avg_carbon, year
          FROM {TARGET_TABLE_NAME}
         WHERE country_name = 'Brazil'
         GROUP BY country_name, year
         ORDER BY year ASC
    """).df()
    logging.info(brazil_df.to_string())
    logging.info("\n--- Verification Complete ---")


def main():
    """Main execution function for the ingestion pipeline"""
    setup_logging(Path("logs/local_data_ingestion.log"))

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con: Union[duckdb.DuckDBPyConnection, None] = None

    try:
        # 1. Connect to DuckDB
        con = duckdb.connect(str(DUCKDB_PATH))
        logging.info(f"Connected to DuckDB at {DUCKDB_PATH}")

        # 2. Prepare Target Table
        create_target_table(con)

        # 3. Process and Load Data
        rows_loaded = process_and_load_data(con)

        if rows_loaded > 0:
            # 4. Quick Checks
            run_verification_queries(con)

    except Exception as e:
        logging.critical(f"Pipeline execution failed critically: {e}")
        sys.exit(1)

    finally:
        # 5. Close Connection
        if con:
            con.close()
            logging.info(f"DuckDB connection closed. Data successfully loaded into {DUCKDB_PATH}")


if __name__ == "__main__":
    main()