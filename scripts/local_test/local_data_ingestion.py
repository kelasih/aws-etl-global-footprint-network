import logging
import sys
from pathlib import Path
import duckdb
import polars as pl

# Config Logging
LOG_FILE = Path("logs/local_data_ingestion.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE,'w'),
        logging.StreamHandler()
    ]
)

# Paths
RAW_DATA_DIR = Path("local_storage/raw")
DUCKDB_PATH = Path("local_storage/footprint.duckdb")
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
try:
    con = duckdb.connect(DUCKDB_PATH)
    logging.info(f"Connected to DuckDB at {DUCKDB_PATH}")
except Exception as e:
    logging.error(f"Failed to connect to DuckDB: {e}")
    sys.exit(1)

# Create the Target Table
try:
    con.execute("""
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
    """)
    logging.info("Table 'carbon_footprint' is ready (truncated for clean ETL).")
except Exception as e:
    logging.error(f"Failed to create/truncate table: {e}")
    con.close()
    sys.exit(1)

# Collect JSON Files
json_files = sorted(RAW_DATA_DIR.glob("*.json"))
if not json_files:
    logging.warning("No JSON files found in the raw data directory.")
    con.close()
    sys.exit(0)

# Read and Concatenates JSON Files
df_list = []
for json_file in json_files:
    try:
        logging.info(f"Processing {json_file.name}...")
        df_list.append(pl.read_json(json_file))
    except Exception as e:
        logging.error(f"Failed to read {json_file}: {e}")

if not df_list:
    logging.error("No JSON data could be read. Exiting.")
    con.close()
    sys.exit(1)

full_df = pl.concat(df_list, rechunk=True)

# Rename Columns
full_df = full_df.rename({
    "countryCode"  : "country_code",
    "countryName"  : "country_name",
    "cropLand"     : "crop_land",
    "grazingLand"  : "grazing_land",
    "forestLand"   : "forest_land",
    "fishingGround": "fishing_ground",
    "builtupLand"  : "builtup_land"
})

# Load into DuckDB
try:
    con.register("full_df", full_df)
    con.execute("INSERT INTO carbon_footprint SELECT * FROM full_df")
    logging.info(f"Loaded {full_df.shape[0]} records into DuckDB.")
except Exception as e:
    logging.error(f"Failed to insert data into DuckDB: {e}")
    con.close()
    sys.exit(1)

# Quick Checks
try:
    logging.info("Sample records:")
    logging.info(con.execute("SELECT * FROM carbon_footprint LIMIT 10").fetchall())

    logging.info("Total records count:")
    logging.info(con.execute("SELECT count(1) FROM carbon_footprint").fetchall())

    logging.info("Brazil carbon footprint evolution:")
    logging.info(con.execute("""
        SELECT country_name, avg(carbon) AS avg_carbon, year
        FROM carbon_footprint
        WHERE country_name = 'Brazil'
        GROUP BY country_name, year
        ORDER BY year ASC
    """).fetchall())
except Exception as e:
    logging.error(f"Failed during verification queries: {e}")

# Close Connection
con.close()
logging.info(f"Data successfully loaded into {DUCKDB_PATH}")
