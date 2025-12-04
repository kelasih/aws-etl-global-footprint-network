# ETL for Global Footprint Network Data

- This repository contains a Python-based ETL (Extract, Transform, Load) pipeline designed to retrieve data from the Global Footprint Network API for local testing and analysis. 
- The project includes scripts for asynchronous data extraction, and is currently being developed for future integration with Amazon Web Services (AWS).

## About the Global Footprint Network

- The Global Footprint Network is a research organization that provides data and tools to help countries, cities, and individuals understand and manage their ecological resources.
- The Ecological Footprint is a key metric that measures the demand on and supply of nature. It tracks the use of six categories of productive surface areas: cropland, grazing land, fishing grounds, built-up land, forest area, and carbon demand on land.
- This project utilizes the Global Footprint Network's open data platform to extract this valuable information for further analysis.

## Features

This local ETL pipeline includes the following key features:

| Feature | Description |
| :--- | :--- |
| **Asynchronous Data Extraction** | Leverages `aiohttp` and `asyncio` for efficient, non-blocking API calls to the Global Footprint Network API. |
| **Robust Error Handling** | Implements an exponential backoff and retry mechanism to handle transient network issues and API errors gracefully. |
| **Configurable Rate Limiting** | A semaphore is used to limit the number of concurrent API requests, preventing the client from overwhelming the API server. |
| **Local Data Caching** | Raw data extracted from the API is saved locally in JSON format, allowing for inspection and reuse without repeated API calls. |

## Technology Stack

The project is built with the following technologies for local testing:

- **Languages**: Python
- **API**: Global Footprint Network API
- **Database**: DuckDB (for local testing)
- **Key Python Libraries**: `aiohttp`, `asyncio`, `polars`, `duckdb`, `logging`, `json`

## Project Structure

```
.
├── local_test
│   ├── requirements.txt
│   └── scripts
│       ├── local_data_extraction.py
│       └── local_data_ingestion.py
├── mkt_returns
│   ├── calendar.csv
│   ├── data.csv
│   └── sql_test_mkt_returns.ipynb
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
└── aws_etl.drawion
```

## Getting Started

### Prerequisites

- Python 3.9+
- A Global Footprint Network API key.

### Installation

1.  Clone the repository:

    ```bash
    git clone https://github.com/kelasih/aws-etl-global-footprint-network.git
    cd aws-etl-global-footprint-network
    ```

2.  Install the required Python packages:

    ```bash
    pip install -r scripts/local_test/requirements.txt
    ```

### Configuration

Create a `.env` file in the root directory of the project by copying the `.env.example` file. For local testing, you only need to fill in the Global Footprint Network API credentials:

```
# Global Footprint Network API
API_URL=https://api.footprintnetwork.org/v1
API_KEY=YOUR_API_KEY
```

## Usage

- The local ETL process is handled by the script `scripts/local_test/local_data_extraction.py`. This script fetches data from the Global Footprint Network API and saves it to the `local_storage/raw` directory.
- After extracting the data, use the script `scripts/local_test/local_data_ingestion.py` to load it into DuckDB.

## Future Development

This project is currently under development to include full integration with AWS. The planned features include:

<img width="400" height="400" alt="image" src="https://github.com/user-attachments/assets/989b7eb2-0a00-4dc7-8274-84247cd41c6c" />

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## References

Global Footprint Network. Retrieved from [https://www.footprintnetwork.org/](https://data.footprintnetwork.org/#/api)
