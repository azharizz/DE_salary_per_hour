import pandas as pd
from pandas import DataFrame
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_csv(path: str, data_type: str) -> DataFrame:
    """
    Load a CSV file into a pandas DataFrame.

    Args:
        path (str): Path to the CSV file.
        data_type (str): Type of data being loaded (for logging purposes).

    Returns:
        DataFrame: Loaded DataFrame.
    """
    try:
        df = pd.read_csv(path)

        # Add Filter with timestamp - 1 day before


        logger.info(f"Loaded {data_type} data from '{path}' with {len(df)} records.")
        return df
    except Exception as e:
        logger.error(f"Failed to load {data_type} data from '{path}': {e}")
        raise