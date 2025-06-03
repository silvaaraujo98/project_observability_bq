import pandas as pd
import numpy as np
import plotly.express as px
from transformation import run_all_transformation_functions
from auxiliary_functions import *


def plot_average_execution_time(df):
    fig = px.line(
       df,
       x='Clusterized_Date',
       y='execution_time_min',
       color = 'ProjectId',
       title = 'Tempo Médio de Execução')
    
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
  
    
    return fig


if __name__ == '__main__':
  
  df = run_all_transformation_functions()
  df = get_specific_columns(df,'ProjectId','Clusterized_Date','execution_time_min')
  grouped_df  = group_and_aggregate_data(df,'execution_time_min','Clusterized_Date','ProjectId',aggregation_method='mean')
  plot_average_execution_time(grouped_df)