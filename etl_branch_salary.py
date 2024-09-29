import logging
import pandas as pd
from dotenv import load_dotenv
import os

from common_package.extract.extract_pipeline import load_csv
from common_package.transform.transform_pipeline import remove_duplicates, transform_times, adjust_checkout_times, merge_employees_timesheets, aggregate_data
from common_package.load.load_pipeline import load_to_bigquery



# Configure pandas display options
pd.options.display.float_format = '{:.6f}'.format

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




def etl(BIGQUERY_DATASET_ID : str,
        BIGQUERY_MAIN_TABLE_ID : str,
        BIGQUERY_STAGING_TABLE_ID : str
        ) -> None:
    """Main function to execute the data pipeline steps."""
    try:
        load_dotenv()
        logger.info("Starting data processing pipeline.")

        # Load data        
        employees = load_csv('employees.csv', "employees")
        timesheets = load_csv('timesheets.csv', "timesheets")


        # Data cleaning and transformation
        timesheets = remove_duplicates(timesheets)
        timesheets = transform_times(timesheets)
        timesheets = adjust_checkout_times(timesheets)

        # Merge and aggregate data
        final_data = merge_employees_timesheets(timesheets, employees)
        final_data = aggregate_data(final_data)
        logger.info("Completed merging and aggregation.")

        # Upload final data to BigQuery
        # Replace the following variables with your actual BigQuery configuration
        BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
        SERVICE_ACCOUNT_PATH = os.getenv('SERVICE_ACCOUNT_PATH')

        load_to_bigquery(
            df=final_data,
            project_id=BIGQUERY_PROJECT_ID,
            dataset_id=BIGQUERY_DATASET_ID,
            main_table_id=BIGQUERY_MAIN_TABLE_ID,
            staging_table_id=BIGQUERY_STAGING_TABLE_ID,
            credentials_path=SERVICE_ACCOUNT_PATH  
        )

        logger.info("Data processing pipeline completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the data processing pipeline: {e}")


# For running locally
if __name__ == "__main__":
    BIGQUERY_DATASET_ID = 'my_dataset'
    BIGQUERY_MAIN_TABLE_ID = 'branch_salary_python'
    BIGQUERY_STAGING_TABLE_ID = 'stg_branch_salary_python'

    # The pipeline should call function etl this below:
    etl(BIGQUERY_DATASET_ID, BIGQUERY_MAIN_TABLE_ID, BIGQUERY_STAGING_TABLE_ID)
