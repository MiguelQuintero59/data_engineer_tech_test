# Data Engineer Tech Test — Transaction ETL Pipeline

An Apache Beam batch ETL pipeline built in Python that reads transaction data from Google Cloud Storage, applies filtering and aggregation transforms, and outputs the results as compressed JSONL.

---

## Problem Statement

Build a Python-based Apache Beam batch job that:

### Task 1
1. Reads input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
2. Finds all transactions with a `transaction_amount` greater than 20
3. Excludes all transactions made before the year 2010
4. Sums the total amount by date
5. Saves the output to `output/results.jsonl.gz`

### Task 2
1. Groups all transformation steps into a single **Composite Transform**
2. Adds a **unit test** for the Composite Transform using Apache Beam's testing utilities

---

## ETL Process

The pipeline follows a classic **Extract → Transform → Load** pattern:


### Extract
- Reads the CSV file from Google Cloud Storage

### Transform (Composite Transform)
All transform steps are grouped into a single `CompositeTransform` class that extends `beam.PTransform`:
1. **Filter by amount** — keeps only transactions where `transaction_amount > 20`
2. **Filter by date** — excludes transactions with a date before `2010-01-01`
3. **Group by date** — maps each transaction to a `(date, amount)` key-value pair
4. **Sum per key** — aggregates the total amount for each date using `beam.CombinePerKey(sum)`

### Load
- The output is written to `output/results.jsonl.gz`

---

## Tech Stack

| Technology | Purpose |
|---|---|
| **Python 3.11** | Core programming language |
| **Apache Beam** | Distributed data processing framework (DirectRunner) |
| **Docker** | Containerised execution environment |
| **pytest** | Unit testing framework |
| **Apache Beam Testing Utilities** | `TestPipeline`, `assert_that`, `equal_to` for pipeline testing |
| **setuptools** | Project packaging and entry-point management (`pyproject.toml`) |

---

## Project Structure

```
tech_test_vm/
├── Dockerfile                  # Docker image definition (Python 3.11)
├── run.sh                      # Execution script
├── pyproject.toml              # Project metadata, dependencies & entry-points
├── .gitignore
├── .dockerignore
├── src/
│   ├── transactions/
│   │   ├── __init__.py
│   │   ├── main.py             # Main Pipeline execution
│   │   ├── config.py           # Input/output file paths
│   │   ├── config_logger.py    # Logging setup
│   │   └── transform/
│   │       ├── __init__.py
│   │       ├── parser.py       # CSV row parser
│   │       ├── transformation.py   # Filter & formatting functions
│   │       └── composite.py    # CompositeTransform
│   └── test/
│       └── transform/
│           └── test_composite.py   # Unit test for the CompositeTransform
├── output/                     # Pipeline output
└── logs/                       # Pipeline logs
```

---

## Prerequisites

- **Docker** must be installed and **running** on your machine  

---

## How to Run

 **Important:** Make sure Docker Desktop or the Docker daemon is open and running before executing the command below.

### 1. Clone the repository

```bash
git clone https://github.com/MiguelQuintero59/data_engineer_tech_test.git
cd data_engineer_tech_test
```

### 2. Run the pipeline

```bash
bash run.sh
```

This single command will:
1. Pull the pre-built Docker image mquintero27/vm_test:v1.0 from Docker Hub
2. Run the container, which executes the transactions-etl command

## Running Tests

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate

# Install the project with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```
---

Miguel Quintero — [miguelquintero95@gmail.com](mailto:miguelquintero95@gmail.com)