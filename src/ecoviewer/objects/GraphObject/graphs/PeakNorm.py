from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.express as px
from ecoviewer.display.graphhelper import calc_daily_peakyness

class PeakNorm(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Peak Norm", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        events_to_filter=['HW_LOSS']
        df_daily_filtered = dm.get_daily_summary_data_df(self.summary_group, events_to_filter)
        df_hourly_filtered = dm.get_hourly_summary_data_df(self.summary_group, events_to_filter)
        
        df_daily_with_peak = calc_daily_peakyness(df_daily_filtered, df_hourly_filtered, flow_variable=dm.flow_variable)

        if df_daily_with_peak.empty:
            # no data to display
            return None
        
        df_daily_with_peak['Flow_CityWater_PP'] = df_daily_with_peak[dm.flow_variable]  * 60 * 24 / dm.occupant_capacity

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_daily_with_peak['Flow_CityWater_PP'], y=df_daily_with_peak['peak_norm'], mode='markers', marker=dict(color='darkblue')))
        fig.update_layout(title = '<b>Daily Peak Norm')
        fig.update_yaxes(title = '<b>Daily Max Fraction of DHW Consumed in 3-Hour Period')
        fig.update_xaxes(title = '<b>Gallons/Person/Day')

        return dcc.Graph(figure=fig)
    