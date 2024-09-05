from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from datetime import time

class OHPPerformance(GraphObject):
    def __init__(self, dm : DataManager, title : str = "OHP Performance", summary_group : str = None):
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

        power_cols = [col for col in df_merged.columns if 'OHP' in col and 'Power' in col]
        temp_cols = [col for col in df_merged.columns if 'IHP' in col and 'Space_Temp' in col]
        colors = ['darkblue', 'darkred', 'darkolivegreen', 'darkcyan', 'palevioletred', 'lightblue', 'khaki', 'sienna',
                'indianred', 'mediumseagreen', 'goldenrod']

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)

        prefix_color_map = {}
        color_index = [0]  # Use a list to hold the color index

        def get_color_for_prefix(prefix):
            if prefix not in prefix_color_map:
                prefix_color_map[prefix] = colors[color_index[0] % len(colors)]
                color_index[0] += 1
            return prefix_color_map[prefix]


        for power in power_cols:
            prefix = '_'.join(power.split('_')[:-1])  
            color = get_color_for_prefix(prefix)
            name = power.replace('_', ' ')
            line_style = 'dash' if 'active' in power else 'solid'
            
            fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged[power], name = name, marker=dict(color=color), line=dict(dash = line_style)), row = 1, col = 1)

        for temp in temp_cols:
            prefix = '_'.join(temp.split('_')[:-1])  
            color = get_color_for_prefix(prefix)
            name = temp.replace('_', ' ')
            line_style = 'dash' if 'active' in temp else 'solid'

            fig.add_trace(go.Scatter(x = df_merged.index, y = df_merged[temp], name = name, marker=dict(color=color), line=dict(dash = line_style)), row = 2, col = 1)

        fig.update_layout(height=1100)
        fig.update_yaxes(title = '<b>Power (kW)', row = 1, col = 1)
        fig.update_yaxes(title = '<b>Temp(F)', row = 2, col = 1)
        fig.update_layout(title = '<b>OHP Performance: Active vs Passive Mode')

        return dcc.Graph(figure=fig)