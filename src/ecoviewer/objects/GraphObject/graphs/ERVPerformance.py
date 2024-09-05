from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from datetime import time

class ERVPerformance(GraphObject):
    def __init__(self, dm : DataManager, title : str = "ERV Performance", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df = dm.get_daily_summary_data_df(self.summary_group)
        passive = df.loc[df['Active_Mode'] == 1]
        active = df.loc[df['Passive_Mode'] == 1]
        
        average_day_passive = passive.groupby(passive.index.time).mean()
        average_day_active = active.groupby(active.index.time).mean()
        
        average_day_passive.sort_index(inplace = True)
        average_day_active.sort_index(inplace = True)
        
        df_merged = pd.merge(average_day_passive, average_day_active, left_index=True, right_index=True, how='outer', suffixes=('_passive', '_active'))
        df_merged = df_merged.loc[(df_merged.index >= time(7,0)) & (df_merged.index <= time(19,0))]

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05) 
        
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Workspace_East_CO2_passive, name = 'East Workspace CO2 Passive', marker=dict(color='darkblue')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Workspace_West_CO2_passive, name = 'West Workspace CO2 Passive', marker=dict(color='darkred')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Outside_Air_CO2_passive, name = 'Outside Air CO2 Passive', marker=dict(color='darkolivegreen')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.PowerIn_ERV3_passive, name = 'ERV 3 Power Draw Passive', marker=dict(color='lightblue')), row = 2, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.PowerIn_ERV4_passive, name = 'ERV 4 Power Draw Passive', marker=dict(color='darkcyan')), row = 2, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.ERV_3_Supply_Air_Flow_passive, name = "ERV 3 Supply Air Flow Passive", marker=dict(color='goldenrod')), row = 3, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.ERV_4_Supply_Air_Flow_passive, name = "ERV 4 Supply Air Flow Passive", marker=dict(color='palevioletred')), row = 3, col = 1)
        
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Workspace_East_CO2_active, name = 'East Workspace CO2 Active', marker=dict(color='darkblue'), line=dict(dash = 'dash')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Workspace_West_CO2_active, name = 'West Workspace CO2 Active', marker=dict(color='darkred'), line=dict(dash = 'dash')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.Outside_Air_CO2_active, name = 'Outside Air CO2 Active', marker=dict(color='darkolivegreen'), line=dict(dash = 'dash')), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.PowerIn_ERV3_active, name = 'ERV 3 Power Draw Active', marker=dict(color='lightblue'), line=dict(dash = 'dash')), row = 2, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.PowerIn_ERV4_active, name = 'ERV 4 Power Draw Active', marker=dict(color='darkcyan'), line=dict(dash = 'dash')), row = 2, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.ERV_3_Supply_Air_Flow_active, name = "ERV 3 Supply Air Flow Active", marker=dict(color='goldenrod'), line=dict(dash = 'dash')), row = 3, col = 1)
        fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged.ERV_4_Supply_Air_Flow_active, name = "ERV 4 Supply Air Flow Active", marker=dict(color='palevioletred'), line=dict(dash = 'dash')), row = 3, col = 1)
        
        fig.update_yaxes(title = '<b>Power (kW)', row = 2, col = 1)
        fig.update_yaxes(title = '<b>CO2 PPM', row = 1, col = 1)
        fig.update_yaxes(title = "<b>CFM", row = 3, col = 1)
        fig.update_layout(title = '<b>ERV Performance: Active vs Passive Mode')
        fig.update_layout(height=1100)

        return dcc.Graph(figure=fig)