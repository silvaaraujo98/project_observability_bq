import pandas as pd
import numpy as np
import plotly.express as px
from utils.aux_functions import *


def plot_queries_perfomed(df):
  
  fig = px.line(df,
    x = 'clusterized_date',
    y='queries_perfomed',
    color = 'project_id',
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
  
  return fig











