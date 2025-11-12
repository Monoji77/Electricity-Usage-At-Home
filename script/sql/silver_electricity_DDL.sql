-- overwrite existing table
DROP TABLE IF EXISTS silver.average_power_consumption;

CREATE TABLE silver.average_power_consumption AS
    WITH lag_timestamps AS (
        -- prepare lag timestamps for each appliance
        SELECT 
            appliance_name,
            power_consumption_val,
            recorded_at_timestamp,
            LAG(recorded_at_timestamp) OVER (
                PARTITION BY appliance_name 
                ORDER BY recorded_at_timestamp
            ) AS previous_timestamp, 
            EXTRACT(YEAR FROM recorded_at_timestamp) AS recorded_year,
            EXTRACT(MONTH FROM recorded_at_timestamp) AS recorded_month
        FROM 
            bronze.electricity_consumption
    )

    /*
        FUNCTION: Calculate continuous usage in 5-minute intervals per appliance per month
        ASSUMPTIONS: A single 5 min record is errorneous,
    */
    SELECT 
        appliance_name,
        recorded_year || '-' || LPAD(recorded_month::TEXT, 2, '0') AS year_month,
        SUM(CASE WHEN (recorded_at_timestamp - previous_timestamp <= INTERVAL '6 minutes') THEN 5 END) AS continuous_usage
    FROM lag_timestamps
    GROUP BY appliance_name, year_month
    ORDER BY appliance_name, year_month;

-- display last 3 records per appliance
SELECT * FROM silver.average_power_consumption
WHERE year_month = (
    SELECT MAX(year_month) 
    FROM silver.average_power_consumption
)