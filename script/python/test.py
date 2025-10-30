from utilities.connect_to_database import connect_db
import pandas as pd

# connect to database
engine = connect_db()
sql_query = "SELECT * FROM electricity_consumption;"

data = pd.read_sql(sql_query, engine)

col_name = data.columns.tolist()
col_name.pop(0)

total_usage_query = f"""
    WITH ordered_appliance AS (
        SELECT 
            appliance_name, 
            power_consumption_val, 
            recorded_at_timestamp,
            LAG(recorded_at_timestamp) OVER(PARTITION BY appliance_name ORDER BY recorded_at_timestamp) AS previous_timestamp
        FROM electricity_consumption
        ORDER BY appliance_name, recorded_at_timestamp
    )
    
    -- Calculate total usage time per appliance
    SELECT 
        appliance_name,
        COUNT(appliance_name) * 5 AS total_usage_minutes 
    FROM ordered_appliance
    WHERE recorded_at_timestamp - previous_timestamp < INTERVAL '6 minutes'
    GROUP BY appliance_name;
"""

total_usage_df = pd.read_sql(total_usage_query, engine)
unique_appliance_names_list = total_usage_df['appliance_name']
total_usage_per_appliance_list = total_usage_df['total_usage_minutes']
 
print(total_usage_df)
