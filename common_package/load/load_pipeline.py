import pandas as pd
from pandas import DataFrame
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_to_bigquery(
    df: DataFrame,
    project_id: str,
    dataset_id: str,
    main_table_id: str,
    staging_table_id: str,
    credentials_path: str = None
) -> None:
    """
    Upload a pandas DataFrame to a BigQuery table using a staging table and delete-insert strategy
    based on year, month, and branch_id.

    Args:
        df (DataFrame): The DataFrame to upload.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        main_table_id (str): BigQuery main table ID.
        staging_table_id (str): BigQuery staging table ID.
        credentials_path (str, optional): Path to the service account JSON key file. If not provided, default credentials are used.
    """
    logger.info(f"Uploading data to BigQuery. Project: '{project_id}', Dataset: '{dataset_id}'.")

    try:
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = bigquery.Client(credentials=credentials, project=project_id)
            logger.info("Authenticated using provided service account credentials.")
        else:
            client = bigquery.Client(project=project_id)
            logger.info("Authenticated using default credentials.")

        # Define table references
        main_table_ref = f"{project_id}.{dataset_id}.{main_table_id}"
        staging_table_ref = f"{project_id}.{dataset_id}.{staging_table_id}"

        # Upload the DataFrame to the staging table with WRITE_TRUNCATE to overwrite existing staging data
        logger.info(f"Uploading data to staging table '{staging_table_ref}'.")
        job = client.load_table_from_dataframe(df, staging_table_ref, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()  # Wait for the upload to complete
        logger.info(f"Successfully uploaded data to staging table '{staging_table_ref}'.")

        # Delete existing records in the main table that match year, month, branch_id in the staging table
        delete_query = f"""
        DELETE FROM `{main_table_ref}` AS main
        WHERE EXISTS (
            SELECT 1
            FROM `{staging_table_ref}` AS staging
            WHERE main.year = staging.year
            AND main.month = staging.month
            AND main.branch_id = staging.branch_id
        );

        INSERT INTO `{main_table_ref}` (year, month, branch_id, hours_diff, salary, salary_per_hour)
        SELECT year, month, branch_id, hours_diff, salary, salary_per_hour
        FROM `{staging_table_ref}`;
        """

        logger.info("Executing DELETE INSERT statement to remove existing records matching the staging data keys.")
        delete_job = client.query(delete_query)
        delete_job.result()  # Wait for the DELETE job to complete
        logger.info("DELETE INSERT statement executed successfully.")

        logger.info(f"Successfully delete inserted {len(df)} records into main table '{main_table_ref}'.")

        # Clean up the staging table if not needed
        logger.info(f"Deleting staging table '{staging_table_ref}'.")
        client.delete_table(staging_table_ref)
        logger.info(f"Staging table '{staging_table_ref}' deleted successfully.")

    except Exception as e:
        logger.error(f"Failed to upload data to BigQuery: {e}")
        raise