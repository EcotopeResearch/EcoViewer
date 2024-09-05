from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
import plotly.graph_objects as go
from dash import dcc
import plotly.express as px

class SummaryDailyPowerByHour(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Average Daily Power Graph", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df = dm.get_daily_summary_data_df(self.summary_group)
        hourly_df = dm.get_hourly_summary_data_df(self.summary_group)
        powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and df[col].dtype == "float64"]

        nls_df = hourly_df[hourly_df['load_shift_day'] == 0]
        ls_df = hourly_df[hourly_df['load_shift_day'] == 1]

        ls_df = ls_df.groupby('hr').mean(numeric_only = True)
        ls_df = dm.round_df_to_3_decimal(ls_df)

        nls_df = nls_df.groupby('hr').mean(numeric_only = True)
        nls_df = dm.round_df_to_3_decimal(nls_df)

        power_df = hourly_df.groupby('hr').mean(numeric_only = True)
        power_df = dm.round_df_to_3_decimal(power_df)

        power_fig = px.line(title = "<b>Average Daily Power")
        
        for column_name in powerin_columns:
            if column_name in power_df.columns:
                trace = go.Scatter(x=power_df.index, y=power_df[column_name], name=f"{column_name}", mode='lines')
                power_fig.add_trace(trace)
                trace = go.Scatter(x=ls_df.index, y=ls_df[column_name], name=f"Load Shift Day {column_name}", mode='lines')
                power_fig.add_trace(trace)
                trace = go.Scatter(x=nls_df.index, y=nls_df[column_name], name=f"Normal Day {column_name}", mode='lines')
                power_fig.add_trace(trace)

        power_fig.update_layout(
            # width=1300,
            yaxis1=dict(
                title='<b>kW',
            ),
            xaxis=dict(
                title='<b>Hour',
            ),
            legend=dict(x=1.2),
            margin=dict(l=10, r=10),
        )
        
        return dcc.Graph(figure=power_fig)