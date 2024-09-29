
CREATE OR REPLACE TABLE `datahub-project-430801.my_dataset.target_tables` 
CLUSTER BY branch_id
AS 
WITH DeduplicatedTimesheets AS (
    SELECT 
        date,
        employee_id,
        DATETIME(date, checkin) AS checkin_t,
        DATETIME(date, checkout) AS checkout_t, 
        ROW_NUMBER() OVER(PARTITION BY employee_id, date ORDER BY timesheet_id ASC) AS row_num
    FROM `datahub-project-430801.my_dataset.timesheets`
),

ProcessedTimesheets AS (
    SELECT 
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        t.employee_id,
        CASE
            WHEN checkin_t IS NULL AND TIME(checkout_t) <= TIME "09:00:00" THEN DATETIME(DATE(checkout_t), TIME "00:00:00")
            WHEN checkin_t IS NULL AND TIME(checkout_t) > TIME "09:00:00" THEN DATETIME(DATE(checkout_t), TIME "09:00:00")
            ELSE checkin_t
        END AS checkin_clean,
        CASE
            WHEN checkout_t IS NULL AND TIME(checkin_t) <= TIME "12:00:00" THEN DATETIME(DATE(checkin_t), TIME "18:00:00")
            WHEN checkout_t IS NULL AND TIME(checkin_t) > TIME "12:00:00" THEN DATETIME(DATE_ADD(DATE(checkin_t), INTERVAL 1 DAY), TIME "08:00:00")
            WHEN checkout_t < checkin_t THEN DATETIME_ADD(checkout_t, INTERVAL 1 DAY)
            ELSE checkout_t
        END AS checkout_clean,
        e.branch_id,
        e.salary
    FROM DeduplicatedTimesheets t
    LEFT JOIN `datahub-project-430801.my_dataset.employees` e 
        ON t.employee_id = e.employe_id
    WHERE row_num = 1
),

AggregatedMonthly AS (
    SELECT 
        year, 
        month, 
        branch_id, 
        salary, 
        SUM(DATETIME_DIFF(checkout_clean, checkin_clean, HOUR)) AS total_hours
    FROM ProcessedTimesheets
    GROUP BY year, month, branch_id, salary
)

SELECT 
    year, 
    month, 
    branch_id, 
    SUM(salary) / SUM(total_hours) AS salary_per_hour
FROM AggregatedMonthly
GROUP BY year, month, branch_id;
