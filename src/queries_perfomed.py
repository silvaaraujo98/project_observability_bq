import pandas as pd
import numpy as np
import plotly.express as px
from transformation import run_all_transformation_functions
from auxiliary_functions import *

df = run_all_transformation_functions()

def plot_queries_perfomed(df):
  
  fig = px.line(df,
    x = 'Clusterized_Date',
    y='Queries',
    color = 'ProjectId',
    title = 'Consultas Realizadas')
  
  fig.update_layout(
    xaxis=dict(
        tickformat='%d-%m %H:%M', # Format the time axis
        tickangle=45,
    ),
    yaxis=dict(
        title_font_size=12,
    ),
    legend=dict(
        title_font_size=12,
        traceorder='normal',
    ),
    margin=dict(l=20, r=20, t=40, b=20),  # Adjust margins
    plot_bgcolor='white', # Set the plot background color
    )
  
  fig.show()

if __name__ == '__main__':
  df = get_specific_columns(df,'ProjectId','Clusterized_Date','Queries')
  grouped_df  = group_and_aggregate_data(df,'Queries','ProjectId','Clusterized_Date')
  plot_queries_perfomed(grouped_df)










