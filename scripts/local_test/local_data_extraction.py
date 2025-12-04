import os
import json
import asyncio
import aiohttp
import random
import logging
from pathlib import Path
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


# --- Configuration using Dataclass ---
@dataclass # Used for holding our data, it auto implements important methods like __init__
class APIConfig:
    """Stores all configuration settings for the API extraction"""
    api_url: str
    api_key: str
    raw_data_dir: Path = Path("local_storage/raw")
    log_file: Path = Path("logs/local_data_extraction.log")
    max_concurrent: int = 5  # Semaphore limit for API calls
    max_retries: int = 5
    initial_delay: float = 1.0  # Initial wait time for backoff
    timeout_seconds: float = 30.0  # Total timeout for a single request
    # Years to fetch
    years_to_fetch: List[int] = field(default_factory=lambda: list(range(2000, 2024 + 1))) # Way to provide default values to list in dataclass


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


# --- Core Utilities ---
def increase_delay(delay: float) -> float:
    """Doubles the delay with exponential backoff, capped at 60 seconds"""
    # Adds a small exponential + random delay between 0 and 1
    return min(delay * 2 * random.uniform(0.0, 1.0), 60.0)


def save_local(data: Dict[str, Any], filepath: Path):
    """Saves data to local storage"""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved: {filepath}")
    except IOError as e:
        logging.error(f"Failed to save file {filepath}: {e}")


# --- API Fetching Logic ---
async def fetch_year(
        session: aiohttp.ClientSession,
        year: int,
        semaphore: asyncio.Semaphore,
        config: APIConfig
) -> Optional[str]: # It returns None if it's successful, but may return an error message if it fails
    """
    Fetches data for a specific year from the API with retries and backoff
    Returns None on success or a string error message on failure
    """
    endpoint = f"{config.api_url}/data/all/{year}"
    filename = f"data_all_{year}.json"
    filepath = config.raw_data_dir / filename

    if filepath.exists():
        logging.info(f"Year {year}: already exists, skipping...")
        return None

    delay = config.initial_delay

    for attempt in range(1, config.max_retries + 1):
        try:
            async with semaphore:
                timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)

                async with session.get(
                        endpoint,
                        headers={"Accept": "application/json"},
                        timeout=timeout
                ) as resp:

                    # Handle ALL Server Errors (5xx) and Rate Limiting (429) for retrying
                    if resp.status == 429 or 500 <= resp.status <= 599:
                        logging.warning(
                            f"Year {year}: attempt {attempt}/{config.max_retries}, Status {resp.status}, waiting {delay:.1f}s…"
                        )
                        await asyncio.sleep(delay)
                        delay = increase_delay(delay)
                        continue  # Go to next attempt

                    # Handle permanent errors
                    if 400 <= resp.status < 500:
                        resp.raise_for_status()  # Will be caught by the outer exception handler

                    # Success check
                    if resp.ok:
                        data = await resp.json()  # This may raise json.JSONDecodeError if content is bad

                        save_local(data, filepath)
                        logging.info(f"Fetched {year} successfully")

                        # Add sleep after success in order to not stress the API
                        await asyncio.sleep(random.uniform(0.1, 0.5))
                        return None  # Success

        except aiohttp.ClientResponseError as e:
            # Catch specific errors like 404, 401, etc. They are permanent failures
            logging.error(f"Year {year}: Permanent HTTP Error: {e.status} - {e.message}. Stopping retries")
            return f"Permanent HTTP Error: {e.status} - {e.message}"

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            # Catch transient network/decoding errors
            logging.warning(
                f"Year {year}: attempt {attempt}/{config.max_retries} failed: {type(e).__name__} - {e}, retrying in {delay:.1f}s"
            )
            await asyncio.sleep(delay)
            delay = increase_delay(delay)

    # If the loop finishes without success
    logging.error(f"Failed to fetch year {year} after {config.max_retries} attempts")
    return "Max retries exceeded"


async def main():
    """Main function to orchestrate the asynchronous fetching process"""

    # Load environment variables
    load_dotenv()
    api_url = os.getenv("API_URL")
    api_key = os.getenv("API_KEY")

    # Initialize configuration
    if not api_url or not api_key:
        raise ValueError(f"API_URL or API_KEY not set in environment")

    config = APIConfig(api_url=api_url, api_key=api_key)

    # Calculate BasicAuth once
    global_auth_header = aiohttp.BasicAuth("any-user", config.api_key)

    # Setup logging and directories
    setup_logging(config.log_file)
    config.raw_data_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Starting extraction for years {config.years_to_fetch[0]} to {config.years_to_fetch[-1]}...")
    logging.info(f"Max concurrent connections: {config.max_concurrent}")

    semaphore = asyncio.Semaphore(config.max_concurrent)
    failures = 0
    total_years = len(config.years_to_fetch)

    async with aiohttp.ClientSession(auth=global_auth_header) as session:
        tasks = [fetch_year(session, year, semaphore, config) for year in config.years_to_fetch]

        # Gather results and exceptions
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, str):
                failures += 1
            # Handle unexpected exceptions during gather
            elif isinstance(result, Exception):
                logging.critical(f"Unhandled exception during task gathering: {result}")
                failures += 1

    # Final summary
    successes = total_years - failures
    logging.info(f"\n--- Extraction Summary ---")
    logging.info(f"Total years targeted: {total_years}")
    logging.info(f"Successful fetches (or skipped): {successes}")
    logging.info(f"Failed fetches: {failures}")

    if failures > 0:
        logging.error(f"Pipeline finished with errors. Check logs for details")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script interrupted by user (KeyboardInterrupt). Exiting gracefully")
    except Exception as e:
        logging.critical(f"A critical, unhandled error occurred: {e}")