import pandas as pd
import numpy as np
import plotly.express as px


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
    
    return fig



