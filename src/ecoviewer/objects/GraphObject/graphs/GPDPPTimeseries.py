from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.express as px

class GPDPPTimeseries(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Daily Hot Water Usage Graph", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=['HW_LOSS'])

        df_daily['Flow_CityWater_Total'] = df_daily[dm.flow_variable] * (60 * 24) #average GPM * 60min/hr * 24hr/day
        df_daily['Flow_CityWater_PP'] = round(df_daily['Flow_CityWater_Total'] / dm.occupant_capacity, 2)
        percentile = 0.95
        mean_day = df_daily[dm.flow_variable].mean() * 24 * 60
        percentile_day = df_daily[dm.flow_variable].quantile(percentile) * 24 * 60
        mean_daily_usage = mean_day / dm.occupant_capacity
        high_daily_usage = percentile_day / dm.occupant_capacity 

        fig = go.Figure()
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily.Flow_CityWater_PP, mode = 'markers',
                                marker = dict(size=5, color = 'darkblue'), showlegend=False))
        
        fig.add_trace(go.Scatter(
        x=[df_daily.index.min() - pd.Timedelta(hours = 23), df_daily.index.max() + pd.Timedelta(hours = 23)],
        y=[mean_daily_usage, mean_daily_usage],
        mode="lines",
        line=dict(color="darkred", dash="dash"),
        name="Mean Daily Usage",
        hoverinfo="text",
        hovertext=f"Mean Daily Usage: {mean_daily_usage:.2f} Gallons/Person/Day"))
        
        fig.add_trace(go.Scatter(
        x=[df_daily.index.min() - pd.Timedelta(hours = 23), df_daily.index.max() + pd.Timedelta(hours = 23)],
        y=[high_daily_usage, high_daily_usage],
        mode="lines",
        line=dict(color="darkgreen", dash="dash"),
        name="95th Percentile Usage",
        hoverinfo="text",
        hovertext=f"95th Percentile Usage: {high_daily_usage:.2f} Gallons/Person/Day"))

        fig.update_layout(title = '<b>Daily Hot Water Usage')
        fig.update_yaxes(title = '<b>Gallons/Person/Day')
        fig.update_xaxes(title = '<b>Time')

        return dcc.Graph(figure=fig)