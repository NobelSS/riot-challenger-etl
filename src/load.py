import os
import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)

def load_to_parquet(df: pd.DataFrame, file_path: str = "data/challenger_mastery.parquet") -> None:
    """Saves the transformed DataFrame to a Parquet file."""
    if df.empty:
        logger.warning("DataFrame is empty. Skipping load process.")
        return

    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    try:
        logger.info(f"Loading data to {file_path}...")
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logger.info("Data successfully loaded.")
    except Exception as e:
        logger.error(f"Failed to load data to Parquet: {e}")
        raise