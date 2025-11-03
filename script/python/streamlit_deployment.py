from pyecharts import options as opts
from pyecharts.charts import Bar
from streamlit_echarts5 import st_pyecharts
import streamlit as st
from utilities.connect_to_database import connect_db
import pandas as pd
import plotly.express as px

######## GLOBAL VARIABLES #######
APPLIANCE_NAME = 'appliance_name'
TOTAL_USAGE_MINUTES = 'total_usage_minutes'
# connect to database
engine = connect_db()
sql_query = "SELECT * FROM electricity_consumption;"

data = pd.read_sql(sql_query, engine)

# obtain total usage per appliance
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
    GROUP BY appliance_name
    ORDER BY COUNT(appliance_name) * 5 ;
"""

total_usage_df = pd.read_sql(total_usage_query, engine)
# unique_appliance_names_list = total_usage_df['appliance_name'].tolist()
# total_usage_per_appliance_list = total_usage_df['total_usage_minutes'].tolist()
# Horizontal bar chart
# Streamlit multiselect for filtering categories
selected_fruits = st.multiselect(
    "Select appliance for display:",
    options=total_usage_df[APPLIANCE_NAME],
    default=total_usage_df[APPLIANCE_NAME]  # show all by default
)

filtered_df = total_usage_df[total_usage_df[APPLIANCE_NAME].isin(selected_fruits)]

# Create horizontal bar chart
fig = px.bar(
    filtered_df,
    x=TOTAL_USAGE_MINUTES,
    y=APPLIANCE_NAME,
    orientation='h',
    color=APPLIANCE_NAME,
    text=TOTAL_USAGE_MINUTES,
    title='Total Usage Time per Appliance (in minutes)'
)

fig.update_layout(
    yaxis={'categoryorder':'total ascending'},  # sort bars
    xaxis_title='Quantity',
    yaxis_title='Fruits'
)


# streamlit app
st.title("Electricity consumption in the Yong household")

# Display in Streamlit
st.plotly_chart(fig, use_container_width=True)
