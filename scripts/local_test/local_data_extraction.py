import os
import json
import asyncio
import aiohttp
import random
import logging
from pathlib import Path
from dotenv import load_dotenv

# Config Logging
LOG_FILE = Path("logs/local_data_extraction.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,  # Minimum level to log
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),
        logging.StreamHandler()
    ]
)

# Config
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

if not API_URL:
    raise ValueError("API_URL not set")
if not API_KEY:
    raise ValueError("API_KEY not set")

# Path to store raw data
RAW_DATA_DIR = "local_storage/raw"
Path(RAW_DATA_DIR).mkdir(parents=True, exist_ok=True)

# Config the API calls
MAX_CONCURRENT = 2 # Global Footprint API is strict, so 2 is good for not breaking it
MAX_RETRIES = 5
INITIAL_DELAY = 1


def increase_delay(delay):
    """Doubles the delay with exponential backoff, capped at 60 seconds"""
    return min(delay * 2 * (0.5 + random.random()), 60)

def save_local(data, filepath):
    """Saves data to local storage"""

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logging.info(f"Saved: {filepath}")


async def fetch_year(session, year, semaphore):
    """Fetches data from API"""

    endpoint = f"{API_URL}/data/all/{year}"
    filename = f"data_all_{year}.json"
    filepath = Path(RAW_DATA_DIR) / filename

    if filepath.exists():
        logging.info(f"{year} already exists, skipping...")
        return None

    delay = INITIAL_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:

                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(
                    endpoint,
                    auth=aiohttp.BasicAuth("any-user", API_KEY), # The API doesn't require an user
                    headers={"Accept": "application/json"},
                    timeout=timeout
                ) as resp:

                    if resp.status in [429, 500, 502, 503, 504]:
                        logging.info(f"Year {year}: attempt {attempt}/{MAX_RETRIES}, waiting {delay:.1f}s…")
                        await asyncio.sleep(delay)
                        delay = increase_delay(delay)
                        continue

                    resp.raise_for_status() # Raise if status not acceptable for retrying, e.g. 404
                    data = await resp.json()
                    save_local(data, filepath)
                    logging.info(f"Fetched {year}")
                    await asyncio.sleep(random.uniform(0.1, 0.5)) # Even with Semaphore it's better to add a short sleep to avoid hurting the API
                    return None

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logging.warning(f"[WARN] Year {year}: attempt {attempt}/{MAX_RETRIES} failed: {e}, retrying in {delay:.1f}s")
            await asyncio.sleep(delay)
            delay = increase_delay(delay)


    logging.warning(f"Failed to fetch year {year}")
    return None


async def main(years):
    """Main function"""

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_year(session, year, semaphore) for year in years]

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logging.info("Tasks cancelled, exiting...")


if __name__ == "__main__":
    """Main function"""

    years = list(range(2000, 2024 + 1))
    asyncio.run(main(years))