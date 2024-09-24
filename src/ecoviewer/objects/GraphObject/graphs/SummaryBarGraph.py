from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
import math
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.express as px
from datetime import datetime

class SummaryBarGraph(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Energy and COP Bar Graph", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def _format_x_axis_date_str(self, dt_1 : datetime, dt_2 : datetime = None) -> str:
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        # Extract date components
        day_1 = dt_1.day
        month_1 = months[dt_1.month - 1]
        year_1 = dt_1.year

        if dt_2 is None:
            return f"{month_1} {day_1}, {year_1}"
        
        day_2 = dt_2.day
        month_2 = months[dt_2.month - 1]
        year_2 = dt_2.year
        
        # Check if the two dates are in the same year
        if year_1 == year_2:
            # Check if the two dates are in the same month
            if month_1 == month_2:
                return f"{month_1} {day_1} - {day_2}, {year_1}"
            else:
                return f"{month_1} {day_1} - {month_2} {day_2}, {year_1}"
        else:
            return f"{month_1} {day_1}, {year_1} - {month_2} {day_2}, {year_2}"

    def create_graph(self, dm : DataManager):
        # Filter columns with the prefix "PowerIn_" and exclude "PowerIn_Total"
        og_df = dm.get_daily_summary_data_df(self.summary_group)
        powerin_columns = [col for col in og_df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and og_df[col].dtype == "float64"]
        cop_columns = [col for col in og_df.columns if 'COP' in col]
        if len(powerin_columns) == 0 and len(cop_columns) == 0:
            raise Exception("no power or COP data to display.")

        df = og_df[powerin_columns+cop_columns].copy()
        # compress to weeks if more than 3 weeks selected
        compress_to_weeks = False
        formatting_time_delta = min(4, math.floor(24/(len(cop_columns) +1))) # TODO error if there are more than 23 cop columns
        if df.index[-1] - df.index[0] >= pd.Timedelta(weeks=3):
            compress_to_weeks = True
            # calculate weekly COPs
            sum_df = df.copy()
            sum_df['power_sum'] = sum_df[powerin_columns].sum(axis=1)
            for cop_column in cop_columns:
                sum_df[f'heat_out_{cop_column}'] = sum_df['power_sum'] * sum_df[cop_column]
            sum_df = sum_df.resample('W').sum()
            df = df.resample('W').mean()
            for cop_column in cop_columns:
                df[cop_column] = sum_df[f'heat_out_{cop_column}'] / sum_df['power_sum']
            df = dm.round_df_to_x_decimal(df, 3)

            formatting_time_delta = formatting_time_delta * 7

        # x_axis_ticktext = []
        x_axis_tick_val = []
        x_axis_tick_text = []
        x_val = df.index[0]
        while x_val <= df.index[-1]:
            x_axis_tick_val.append(x_val)# + pd.Timedelta(hours=(formatting_time_delta * math.floor(len(cop_column)/2))))
            if compress_to_weeks:
                first_date = x_val - pd.Timedelta(days=6)
                last_date = x_val
                if first_date < og_df.index[0]:
                    first_date = og_df.index[0]
                if x_val > og_df.index[-1]:
                    last_date = og_df.index[-1]
                x_axis_tick_text.append(self._format_x_axis_date_str(first_date, last_date))
                x_val += pd.Timedelta(weeks=1)
            else:
                x_axis_tick_text.append(self._format_x_axis_date_str(x_val))
                x_val += pd.Timedelta(days=1)

        energy_dataframe = df[powerin_columns].copy()
        # Multiply all values in the specified columns by 24
        energy_dataframe[powerin_columns] = energy_dataframe[powerin_columns].apply(lambda x: x * 24)

        # TODO error for no power columns
        # Create a stacked bar graph using Plotly Express
        power_colors = dm.get_color_list(powerin_columns)
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(powerin_columns, True)
        for power_column in powerin_columns:
            energy_dataframe[power_pretty_names_dict[power_column]] = energy_dataframe[power_column]

        cop_colors = dm.get_color_list(cop_columns, i=len(powerin_columns)) # start color index after power columns to avoid color conflict
        stacked_fig = px.bar(energy_dataframe, x=energy_dataframe.index, y=power_pretty_names, color_discrete_sequence=power_colors, title='<b>Energy and COP',
                    labels={'index': 'Data Point'}, 
                    height=400)
        
        num_data_points = len(df)
        x_shift = pd.Timedelta(hours=formatting_time_delta)  # Adjust this value to control the horizontal spacing between the bars
        x_positions_shifted = [x + x_shift for x in df.index]
        # create fake bar for spacing
        stacked_fig.add_trace(go.Bar(x=x_positions_shifted, y=[0]*num_data_points, showlegend=False))
        stacked_fig.update_layout(
            # width=1300,
            yaxis1=dict(
                title='<b>Avg. Daily kWh' if compress_to_weeks else '<b>kWh',
            ),
            xaxis=dict(
                title='<b>Week' if compress_to_weeks else '<b>Day',
                tickmode = 'array',
                tickvals = x_axis_tick_val,
                ticktext = x_axis_tick_text  
            ),
            margin=dict(l=10, r=10),
            legend=dict(x=1.1)
        )

        # Add the additional columns as separate bars next to the stacks
        if len(cop_columns) > 0:
            cop_pretty_names, cop_pretty_name_dict = dm.get_pretty_names(cop_columns)
            df = dm.round_df_to_x_decimal(df, 2)
            for i in range(len(cop_columns)):
            # for col in cop_columns:
                col = cop_columns[i]
                cop_pretty_name = cop_pretty_name_dict[col]
                x_positions_shifted = [x + x_shift for x in df.index]
                stacked_fig.add_trace(go.Bar(
                    x=x_positions_shifted, 
                    y=df[col], 
                    name=cop_pretty_name, 
                    marker=dict(color=cop_colors[i]),
                    yaxis = 'y2',
                    customdata=np.transpose([x_axis_tick_text, [cop_pretty_name]*len(x_axis_tick_text)]),
                    hovertemplate="<br>".join([
                        "variable=%{customdata[1]}",
                        "time_pt=%{customdata[0]}",
                        "value=%{y}",
                    ])
                    ))
                x_shift += pd.Timedelta(hours=formatting_time_delta)
            # create fake bar for spacing
            stacked_fig.add_trace(go.Bar(x=df.index, y=[0]*num_data_points, showlegend=False, yaxis = 'y2'))
            # Create a secondary y-axis
            stacked_fig.update_layout(
                yaxis2=dict(
                    title='<b>COP',
                    overlaying='y',
                    side='right'
                ),
            )

        return dcc.Graph(figure=stacked_fig)