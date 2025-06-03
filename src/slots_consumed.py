import pandas as pd
import numpy as np
import plotly.express as px
from transformation import run_all_transformation_functions
from auxiliary_functions import *

def plot_slots_consumed(df):

    slots_total=float(df['TotalSlotMin'].sum())
    slots_total_formatted = "{:.2f}".format(slots_total).replace(",",".")
    fig = px.pie(
        df,
        values='TotalSlotMin',
        names ='ProjectId',
        title = 'Slots Consumidos nas Últimas 24 horas',
        hole=.8)
    fig.add_annotation(x=.5, y=.5,
            text=slots_total_formatted,
            showarrow=False,
            arrowhead=1,
            font = dict(
                size=20
            ))
    
    fig.show()



if __name__ == '__main__':
  
  df = run_all_transformation_functions()
  df = get_specific_columns(df,'ProjectId','TotalSlotMin')
  grouped_df  = group_and_aggregate_data(df,'TotalSlotMin','ProjectId')
  plot_slots_consumed(grouped_df)