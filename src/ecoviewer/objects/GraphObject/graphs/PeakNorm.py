from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.display.displayutils import get_date_range_string
from ecoviewer.constants.constants import *
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
from ecoviewer.display.graphhelper import calc_daily_peakyness

class PeakNorm(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Peak Norm", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title, event_reports=typical_tracked_events, event_filters=['HW_LOSS'])

    def create_graph(self, dm : DataManager):
        if not 'PARTIAL_OCCUPANCY' in dm.get_ongoing_events():
            self.event_filters.append('PARTIAL_OCCUPANCY')
        df_daily_filtered = dm.get_daily_summary_data_df(self.summary_group, self.event_filters)
        df_hourly_filtered = dm.get_hourly_summary_data_df(self.summary_group, self.event_filters)
        
        df_daily_with_peak = calc_daily_peakyness(df_daily_filtered, df_hourly_filtered, flow_variable=dm.flow_variable)

        if df_daily_with_peak.empty:
            # no data to display
            return None
        units = 'Gallons/Person/Day' if dm.occupant_capacity > 1 else 'Gallons/Day'
        df_daily_with_peak['Flow_CityWater_PP'] = df_daily_with_peak[dm.flow_variable]  * 60 * 24 / dm.occupant_capacity

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_daily_with_peak['Flow_CityWater_PP'], y=df_daily_with_peak['peak_norm'], mode='markers', marker=dict(color='darkblue')))
        fig.update_layout(title = f"<b>Daily Peak Norm<br><span style='font-size:14px;'>{get_date_range_string(df_hourly_filtered)}</span>")
        fig.update_yaxes(title = '<b>Daily Max Fraction of DHW Consumed in 3-Hour Period')
        fig.update_xaxes(title = f'<b>{units}')

        return dcc.Graph(figure=fig)
    