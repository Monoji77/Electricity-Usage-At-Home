from pyecharts import options as opts
from pyecharts.charts import Bar
from streamlit_echarts5 import st_pyecharts
import streamlit as st
from utilities.connect_to_database import connect_db
import pandas as pd


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
unique_appliance_names_list = total_usage_df['appliance_name'].tolist()
total_usage_per_appliance_list = total_usage_df['total_usage_minutes'].tolist()

# streamlit app
st.title("Electricity consumption in the Yong household")
# Example: wrap long labels at 10 characters per line
def wrap_label(label, width=10):
    return "\n".join([label[i:i+width] for i in range(0, len(label), width)])

b = (
    Bar(init_opts=opts.InitOpts(width="1400px", height="600px"))  # 🔑 adjust size here
    .add_xaxis(unique_appliance_names_list)
    .add_yaxis(
        "Total time used (min)", total_usage_per_appliance_list
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(
            title="Most used appliances", subtitle="2025 Oct"
        ),
        toolbox_opts=opts.ToolboxOpts(),
        xaxis_opts=opts.AxisOpts(
            name="Total usage (min)",
            splitline_opts=opts.SplitLineOpts(is_show=False),  # remove vertical gridlines
            axisline_opts=opts.AxisLineOpts(is_on_zero=True),
            axislabel_opts=opts.LabelOpts(margin=20),  # add space so labels are not blocked

        ),
        yaxis_opts=opts.AxisOpts(
            axisline_opts=opts.AxisLineOpts(is_show=True),  # show left axis line
            axislabel_opts=opts.LabelOpts(margin=0,  formatter=lambda x: wrap_label(x, width=10)),  # wrap long labels,  # <-- extra margin
            splitline_opts=opts.SplitLineOpts(is_show=False),  # remove horizontal gridlines
        ),
    )
    .reversal_axis()

)
st_pyecharts(b)
