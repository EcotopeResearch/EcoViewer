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
        power_colors = dm.get_color_list(powerin_columns)

        nls_df = hourly_df[hourly_df['load_shift_day'] == 0]
        ls_df = hourly_df[hourly_df['load_shift_day'] == 1]

        ls_df = ls_df.groupby('hr').mean(numeric_only = True)
        ls_df = dm.round_df_to_x_decimal(ls_df, 3)

        nls_df = nls_df.groupby('hr').mean(numeric_only = True)
        nls_df = dm.round_df_to_x_decimal(nls_df, 3)

        power_df = hourly_df.groupby('hr').mean(numeric_only = True)
        power_df = dm.round_df_to_x_decimal(power_df, 3)

        power_fig = px.line(title = "<b>Average Daily Power")
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(powerin_columns)
        for i in range(len(powerin_columns)):
            column_name = powerin_columns[i]
            if column_name in power_df.columns:
                pretty_name = power_pretty_names_dict[column_name]
                trace = go.Scatter(x=power_df.index, y=power_df[column_name], name=f"{pretty_name}", mode='lines',
                                   line=dict(color=power_colors[i]),)
                power_fig.add_trace(trace)
                # TODO figure out colors for LS and NLS lines
                trace = go.Scatter(x=ls_df.index, y=ls_df[column_name], name=f"Load Shift Day {pretty_name}", mode='lines')
                power_fig.add_trace(trace)
                trace = go.Scatter(x=nls_df.index, y=nls_df[column_name], name=f"Normal Day {pretty_name}", mode='lines')
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