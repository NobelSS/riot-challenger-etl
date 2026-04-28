# Riot Challenger ETL Pipeline

A data pipeline that extracts the top 50 Challenger players from the Riot League of Legends API, aggregates their top champion masteries, transforms the payloads logically, and loads them into a fast, compressed Parquet format.

## Architecture

- **Extraction (`src/extract.py`)**: Fetches data from Riot APIs handling `429` rate limiting with adaptive jitter and `requests.Session()` connection pooling.
- **Transformation (`src/transform.py`)**: Munges the JSON structs using `pandas` and strictly coerces schema fields with `Pydantic`. Maps champion IDs to true names using Data Dragon endpoints.
- **Loading (`src/load.py`)**: Serializes dataframe outputs via `pyarrow` to the `data/` directory for analytical querying.
- **Orchestration**: Fully automated pipeline runs daily at midnight UTC via GitHub Actions, publishing execution artifacts directly to the repository securely.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (For containerized runs)
- [Python 3.11+](https://www.python.org/downloads/) (For local execution)
- A valid [Riot Games API Key](https://developer.riotgames.com/)

## Local Setup

### Bare Metal (Python)

1. Copy `.env.example` to `.env` (or create one):
   ```env
   RIOT_API_KEY=RGAPI-your-api-key-here
   REGION=euw1
   ROUTING_REGION=europe
   ```

2. Generate a local virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```

### Conda Environment

1. Copy `.env.example` to `.env` (or create one):
   ```env
   RIOT_API_KEY=RGAPI-your-api-key-here
   REGION=euw1
   ROUTING_REGION=europe
   ```

2. Create and activate a new Conda environment, then install dependencies:
   ```bash
   conda create -n riot-etl python=3.11 -y
   conda activate riot-etl
   pip install -r requirements.txt
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```

### Docker Container

Build and run using the `Dockerfile`:

```bash
docker build -t riot-challenger-etl .

docker run --rm -e RIOT_API_KEY="RGAPI-your-api-key-here" -e REGION="euw1" -e ROUTING_REGION="europe" -v "$PWD/data:/app/data" riot-etl-test
```

*The generated `.parquet` file will be mapped directly back to the active `./data` directory.*

## Data Visualization (Notebook Preview)

Once the pipeline has run locally and generated the `data/challenger_mastery.parquet` file, you can explore and analyze the dataset using the provided Jupyter Notebook.

1. Open `notebook/preview.ipynb` in VS Code or Jupyter Lab.
2. Run the cells to visualize data such as:
   - The top 10 most common champions used by the top 50 players.
   - Mastery points distribution and outliers.
   - Average mastery points per champion.
   - Brackets showing how many top 50 Challengers fall into specific mastery ranges.

*Note: Make sure your Python or Conda environment is active, as the notebook relies on `pandas`, `matplotlib`, and `seaborn`.*

## Automated Orchestration (GitHub Actions)

This project features a `.github/workflows/run-pipeline.yml` file, automatically executing the ETL process at `00:00 UTC` daily. 

**Configuration required in GitHub:**
1. Navigate to your repository **Settings** > **Secrets and variables** > **Actions**.
2. Create a **New repository secret**.
3. Name it `RIOT_API_KEY` and paste your Riot developer key.

*Pipeline outputs (`challenger_mastery.parquet`) are stored as workflow artifacts for up to 7 days upon success.*

