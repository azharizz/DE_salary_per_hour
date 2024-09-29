import pandas as pd
from pandas import DataFrame
import logging
from typing import List, Tuple
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def remove_duplicates(timesheets: DataFrame) -> DataFrame:
    """
    Remove duplicate timesheets records based on employee_id and date.

    Args:
        timesheets (DataFrame): Timesheets DataFrame containing employee time records.

    Returns:
        DataFrame: Cleaned DataFrame with duplicate records removed.
    """
    logger.info("Removing duplicate timesheet records.")
    duplicates = timesheets[timesheets.duplicated(subset=['employee_id', 'date'], keep=False)]
    index_to_delete = []

    for _, row in duplicates.iterrows():
        # Find duplicates for the current employee_id and date
        mask = (
            (timesheets['employee_id'] == row['employee_id']) &
            (timesheets['date'] == row['date'])
        )
        duplicate_subset = timesheets[mask]

        # Remove timesheets with null checkout first
        null_checkout = duplicate_subset[duplicate_subset['checkout'].isna()]
        if not null_checkout.empty:
            index_to_delete.append(null_checkout.index[0])
            logger.info(f"Marked index {null_checkout.index[0]} for deletion due to null checkout.")
            continue

        # Remove duplicated rows except the first occurrence based on all columns except timesheet_id
        duplicated_except_id = duplicate_subset.drop(['timesheet_id'], axis=1).duplicated(keep='first')
        duplicate_indices = duplicate_subset[duplicated_except_id].index
        if not duplicate_indices.empty:
            index_to_delete.append(duplicate_indices[0])
            logger.info(f"Marked index {duplicate_indices[0]} for deletion due to duplication.")

    cleaned_timesheets = timesheets.drop(index=index_to_delete)
    logger.info(f"Removed {len(index_to_delete)} duplicate timesheet records.")
    return cleaned_timesheets


def process_timesheets_column(
    timesheets: DataFrame,
    column_name: str,
    condition_choices: List[Tuple[List[pd.Series], List[pd.Timedelta]]]
) -> DataFrame:
    """
    Processes a specified timesheet column by converting it to timedelta and filling missing values
    based on conditions and choices.

    Args:
        timesheets (DataFrame): The DataFrame containing timesheet data.
        column_name (str): The name of the column to process ('checkin' or 'checkout').
        condition_choices (List[Tuple[List[pd.Series], List[pd.Timedelta]]]): 
            A list of tuples where each tuple contains a list of conditions and a list of choices for filling missing values.

    Returns:
        DataFrame: The updated DataFrame with processed column.
    """
    logger.info(f"Processing column '{column_name}'.")
    
    # Convert the column to string to handle any direct numeric values
    timesheets[column_name] = timesheets[column_name].astype(str)

    # Iterate over each set of conditions and choices to fill missing values
    for conditions, choices in condition_choices:
        timesheets[column_name] = np.select(conditions, choices, default=timesheets[column_name])

    # Convert the column to timedelta
    timesheets[column_name] = pd.to_timedelta(timesheets[column_name], errors='coerce')

    missing_after = timesheets[column_name].isna().sum()
    if missing_after > 0:
        logger.info(f"Column '{column_name}' has {missing_after} missing values after processing.")

    return timesheets


def transform_times(timesheets: DataFrame) -> DataFrame:
    """
    Convert checkin and checkout columns to timedelta and handle missing values.

    Args:
        timesheets (DataFrame): Timesheets DataFrame with checkin and checkout times.

    Returns:
        DataFrame: DataFrame with checkin and checkout columns transformed to timedelta and missing values filled.
    """
    logger.info("Transforming 'checkin' and 'checkout' columns.")

    # Define conditions and choices for 'checkout'
    checkout_condition_choices = [
        (
            [
                timesheets['checkout'].isna() & (pd.to_timedelta(timesheets['checkin']) <= pd.Timedelta('0 days 12:00:00')),
                timesheets['checkout'].isna() & (pd.to_timedelta(timesheets['checkin']) > pd.Timedelta('0 days 12:00:00'))
            ],
            [
                pd.Timedelta('0 days 18:00:00'),
                pd.Timedelta('1 days 08:00:00')
            ]
        )
    ]

    # Process 'checkout' column
    timesheets = process_timesheets_column(timesheets, 'checkout', checkout_condition_choices)

    # Define conditions and choices for 'checkin'
    checkin_condition_choices = [
        (
            [
                timesheets['checkin'].isna() & (pd.to_timedelta(timesheets['checkout']) <= pd.Timedelta('0 days 09:00:00')),
                timesheets['checkin'].isna() & (pd.to_timedelta(timesheets['checkout']) > pd.Timedelta('0 days 09:00:00'))
            ],
            [
                pd.Timedelta('0 days 00:00:00'),
                pd.Timedelta('0 days 09:00:00')
            ]
        )
    ]

    # Process 'checkin' column
    timesheets = process_timesheets_column(timesheets, 'checkin', checkin_condition_choices)

    logger.info("Completed transforming 'checkin' and 'checkout' columns.")
    return timesheets


def adjust_checkout_times(timesheets: DataFrame) -> DataFrame:
    """
    Adjust checkout times if checkin is later than checkout and calculate time difference.

    Args:
        timesheets (DataFrame): Timesheets DataFrame with checkin and checkout times.

    Returns:
        DataFrame: DataFrame with adjusted checkout times and calculated time differences.
    """
    logger.info("Adjusting checkout times where checkin is later than checkout.")
    
    # Identify records where checkin > checkout
    mask = timesheets['checkin'] > timesheets['checkout']
    adjustment_count = mask.sum()

    if adjustment_count > 0:
        # Adjust checkout by adding one day where necessary
        timesheets.loc[mask, 'checkout'] += pd.Timedelta(days=1)
        logger.info(f"Adjusted checkout times for {adjustment_count} records where checkin > checkout.")
    else:
        logger.info("No checkout times needed adjustment.")

    # Calculate time differences
    timesheets['time_diff'] = timesheets['checkout'] - timesheets['checkin']
    timesheets['hours_diff'] = timesheets['time_diff'].dt.total_seconds() / 3600
    logger.info("Calculated 'time_diff' and 'hours_diff'.")

    return timesheets


def merge_employees_timesheets(timesheets: DataFrame, employees: DataFrame) -> DataFrame:
    """
    Merge timesheets with employees data.

    Args:
        timesheets (DataFrame): Timesheets DataFrame with employee time records.
        employees (DataFrame): Employees DataFrame with employee details.

    Returns:
        DataFrame: Merged DataFrame.
    """
    logger.info("Merging timesheets with employees data.")
    
    # Rename column for consistency
    employees_renamed = employees.rename(columns={'employe_id': 'employee_id'})
    logger.info("Renamed 'employe_id' to 'employee_id' in employees DataFrame.")

    # Perform the merge
    merged_data = pd.merge(timesheets, employees_renamed, on='employee_id', how='left')
    logger.info(f"Merged data contains {len(merged_data)} records.")

    # Extract year and month from date
    merged_data['year'] = pd.to_datetime(merged_data['date']).dt.year
    merged_data['month'] = pd.to_datetime(merged_data['date']).dt.month
    logger.info("Extracted 'year' and 'month' from 'date'.")

    return merged_data


def aggregate_data(merged_data: DataFrame) -> DataFrame:
    """
    Aggregate data by branch, year, and month.

    Args:
        merged_data (DataFrame): Merged DataFrame with employee and timesheet data.

    Returns:
        DataFrame: Aggregated DataFrame with total hours and salary per hour.
    """
    logger.info("Aggregating data by year, month, and branch.")

    # Select relevant columns for aggregation
    aggregated = merged_data.groupby(['year', 'month', 'branch_id', 'salary'], as_index=False).agg({
        'hours_diff': 'sum'
    })

    # Further aggregate by year, month, and branch
    final_aggregated = aggregated.groupby(['year', 'month', 'branch_id'], as_index=False).agg({
        'hours_diff': 'sum',
        'salary': 'sum'
    })

    # Calculate salary per hour
    final_aggregated['salary_per_hour'] = final_aggregated.apply(
        lambda row: row['salary'] / row['hours_diff'] if row['hours_diff'] != 0 else 0,
        axis=1
    )

    logger.info("Aggregated data and calculated 'salary_per_hour'.")
    return final_aggregated