import sys
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
import duckdb
import polars as pl


# --- Configuration using Dataclass ---
@dataclass # Used for holding our data, it auto implements important methods like __init__
class IngestionConfig:
    """Stores all configuration settings for the data ingestion"""

    raw_data_dir: Path = Path("local_storage/raw")
    duckdb_path: Path = Path("local_storage/footprint.duckdb")
    log_file: Path = Path("logs/local_data_ingestion.log")

    # Column mapping for renaming during transformation
    column_mapping: Dict[str, str] = field(default_factory=lambda: {
        "countryCode": "country_code",
        "countryName": "country_name",
        "cropLand": "crop_land",
        "grazingLand": "grazing_land",
        "forestLand": "forest_land",
        "fishingGround": "fishing_ground",
        "builtupLand": "builtup_land"
    })

    # SQL DDL
    target_table_sql: str = """
        CREATE TABLE IF NOT EXISTS carbon_footprint (
            year INTEGER,
            country_code INTEGER,
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
        TRUNCATE TABLE carbon_footprint;
    """


# --- Logging Setup ---
def setup_logging(log_file: Path):
    """Sets up the standardized logging configuration"""

    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler()
        ]
    )


def connect_duckdb(db_path: Path) -> Optional[duckdb.DuckDBPyConnection]:
    """Establishes and returns a connection to DuckDB"""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        con = duckdb.connect(str(db_path), read_only=False)
        logging.info(f"Connected to DuckDB at {db_path}")
        return con
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB at {db_path}: {e}")
        return None


def extract_and_transform(config: IngestionConfig) -> Optional[pl.DataFrame]:
    """Reads and concatenates all JSON files into a single Polars DataFrame"""

    config.raw_data_dir.mkdir(parents=True, exist_ok=True)
    # Collects JSON Files
    json_files: List[Path] = config.raw_data_dir.glob("*.json")
    if not json_files:
        logging.warning(f"No JSON files found in {config.raw_data_dir}. Exiting")
        return None

    # Reads and Concatenates JSON Files
    df_list: List[pl.DataFrame] = []
    for json_file in json_files:
        try:
            logging.info(f"Processing {json_file.name}...")
            # Uses pl.read_json which is efficient for structured JSON
            df_list.append(pl.read_json(json_file))
        except Exception as e:
            logging.error(f"Failed to read {json_file}: {e}")

    if not df_list:
        logging.error("No JSON data could be successfully read. Exiting...")
        return None

    # Concatenates and Transforms data
    full_df = pl.concat(df_list, rechunk=True)

    logging.info(f"Total records loaded from JSON: {full_df.shape[0]}")

    full_df = full_df.rename(config.column_mapping)
    logging.info("DataFrame columns renamed for SQL compatibility")

    return full_df


def load_data(con: duckdb.DuckDBPyConnection, df: pl.DataFrame, sql_setup: str) -> bool:
    """Prepares the database table and loads the DataFrame"""

    try:
        # Setup Table (Creates and Truncates)
        con.sql(sql_setup)
        logging.info("Table 'carbon_footprint' is ready (truncated for clean ETL)")

        # Inserts polars df
        con.sql("INSERT INTO carbon_footprint SELECT * FROM df")
        logging.info(f"Successfully loaded {df.shape[0]} records into DuckDB")
        return True
    except Exception as e:
        logging.error(f"Failed to load data into DuckDB: {e}")
        return False


def run_checks(con: duckdb.DuckDBPyConnection):
    """Executes a few SQL queries to verify the loaded data"""

    try:
        logging.info("\n--- Verification Queries ---")

        logging.info("Total records count:")
        logging.info(con.sql("SELECT count(1) FROM carbon_footprint").fetchone())

        logging.info("Sample records:")
        logging.info(con.sql("SELECT year, country_name, carbon FROM carbon_footprint LIMIT 2").fetchall())

        logging.info("Brazil carbon footprint evolution by Year:")
        logging.info(con.sql("""
            SELECT country_name, avg(carbon) AS avg_carbon, year
              FROM carbon_footprint
             WHERE country_name = 'Brazil'
             GROUP BY country_name, year
             ORDER BY year ASC
             LIMIT 2
        """).fetchall())

    except Exception as e:
        logging.error(f"Failed during verification queries: {e}")


# --- Main ---
def main():
    """Main function to perform the ETL process"""

    config = IngestionConfig()
    setup_logging(config.log_file)

    try:
        # Connects to DB
        con: Union[duckdb.DuckDBPyConnection, None] = connect_duckdb(config.duckdb_path)
        if con is None:
            sys.exit(1)

        # Extracts and Transforms
        full_df = extract_and_transform(config)
        if full_df is None:
            sys.exit(0)  # Exit if no data

        # Loads Data
        if not load_data(con, full_df, config.target_table_sql):
            sys.exit(1)

        # Runs Checks
        run_checks(con)

    except Exception as e:
        # Catches any unexpected critical errors
        logging.critical(f"A critical, unhandled error occurred during ETL: {e}")
        sys.exit(1)

    finally:
        # Closes connection
        if con:
            con.close()
            logging.info(f"\nPipeline finished. Connection closed. Data loaded into {config.duckdb_path}")


if __name__ == "__main__":
    main()