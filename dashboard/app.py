import datetime as dt

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(layout='wide', page_title='Profit and Sales Dashboard')

def get_view_data(view_name):
    # Connect to the database and retrieve the view data in a dataframe
    conn = st.experimental_connection('mysql', type='sql')
    view_df = conn.query(f'SELECT * FROM {view_name};', ttl=60)
    return view_df

def agg_view_data(view_df, groupby_key):
    # Aggregate the sum of sales and profit by the groupby key
    result_df = view_df.groupby(groupby_key).agg({'total_sales': 'sum', 'total_profit': 'sum'}).reset_index()
    return result_df

def visualize_df_by_col(df, col_name, title, color):
    # Visualize the dataframe in a bar chart by the column name
    chart = alt.Chart(df).mark_bar(color=color).encode(
                x=alt.X(col_name, axis=alt.Axis(title='USD')),
                y = alt.Y(df.columns[0], sort='-x', axis=alt.Axis(title=None))
            ).properties(
                height=300,
                width=450,
                title=title)
            
    st.altair_chart(chart, use_container_width=False)
    
today = dt.date.today().strftime("%Y-%m-%d")
title = f"Profit and Sales Dashboard: {today}"
st.header(title)

view_df = get_view_data('sales_and_profit_today')
platform_df = agg_view_data(view_df, "platform_name")
category_df = agg_view_data(view_df, "category_name")

col1, col2, col3 = st.columns([4, 2, 4])

with col1:
    st.metric(label='Total Sales (USD)', value=view_df['total_sales'].sum())
    visualize_df_by_col(category_df, 'total_sales', 'Total Sales by Category Today', 'cyan')
    visualize_df_by_col(platform_df, 'total_sales', 'Total Sales by Platform Today', 'cyan')
    
with col3:
    st.metric(label='Total Profit (USD)', value=view_df['total_profit'].sum())
    visualize_df_by_col(category_df, 'total_profit', 'Total Profit by Category Today', 'orange')
    visualize_df_by_col(platform_df, 'total_profit', 'Total Profit by Platform Today', 'orange')

    
