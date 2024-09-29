import pandas as pd
from pandas import DataFrame
import logging
from datetime import datetime, timedelta

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

        # Add Filter with timestamp - 1 day before for timesheet to reduce data load
        # This is optional, if you need to reduce the data by 1 day before 
        # (Can be specified last update if there is value or get the data first from the bigquery)
        if data_type == 'timesheets':
            one_day_before = datetime.now() - timedelta(days=1)
            df = df[pd.to_datetime(df['date']) > (one_day_before)]

        logger.info(f"Loaded {data_type} data from '{path}' with {len(df)} records.")
        return df
    except Exception as e:
        logger.error(f"Failed to load {data_type} data from '{path}': {e}")
        raise