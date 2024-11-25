from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
import math
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.express as px
from datetime import datetime

class SummaryBarGraphLoadRatios(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Load Ratio Bar Graph", summary_group : str = None):
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
        
        og_df = dm.get_daily_summary_data_df(self.summary_group,['PIPELINE_ERR'])
        
        if og_df.shape[0] <= 0:
            raise Exception("No power or COP data to display for time period.")

        load_columns = ['HeatOut_TM', 'HeatOut_Primary', 'test']
        output_columns = ['HeatOut_HPWH1', 'HeatOut_HPWH2', 'PowerIn_SwingTank']

        load_columns = [col for col in load_columns if col in og_df.columns]
        output_columns = [col for col in output_columns if col in og_df.columns]

        if len(load_columns) == 0 and len(output_columns) == 0:
            raise Exception("No load data to display for time period.")

        df = og_df[load_columns].copy()
        df2 = og_df[output_columns].copy()

        # compress to weeks if more than 3 weeks selected
        compress_to_weeks = False
        formatting_time_delta = min(4, math.floor(24/(len(load_columns) +1))) # TODO error if there are more than 23 cop columns
        if df.index[-1] - df.index[0] >= pd.Timedelta(weeks=3):
            compress_to_weeks = True
            # calculate weekly COPs
            sum_df = df.copy()
            sum_df2 = df2.copy()

            sum_df['load_sum'] = sum_df[load_columns].sum(axis=1)
            sum_df2['output_sum'] = sum_df2[output_columns].sum(axis=1)
  
            sum_df = sum_df.resample('W').sum()
            sum_df2 = sum_df2.resample('W').sum()

            df = df.resample('W').mean()
            df2 = df2.resample('W').mean()

            df = dm.round_df_to_x_decimal(df, 3)
            df2 = dm.round_df_to_x_decimal(df2, 3)
            formatting_time_delta = formatting_time_delta * 7
            
        
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

        energy_dataframe = df[load_columns].copy()
        output_dataframe = df2[output_columns].copy()

        # Multiply all values in the specified columns by 24 to get daily kWh
        energy_dataframe[load_columns] = energy_dataframe[load_columns].apply(lambda x: x * 24)
        output_dataframe[output_columns] = output_dataframe[output_columns].apply(lambda x: x * 24)
        
        # Create a stacked bar graph using Plotly Express
        power_colors = dm.get_color_list(load_columns)
        output_colors = dm.get_color_list(output_columns)
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(load_columns, True)
        output_pretty_names, output_pretty_names_dict = dm.get_pretty_names(output_columns, True)
        
        for power_column in load_columns:
            energy_dataframe[power_pretty_names_dict[power_column]] = energy_dataframe[power_column]
        for output_column in output_columns:
            output_dataframe[output_pretty_names_dict[output_column]] = output_dataframe[output_column]

        stacked_fig = px.bar(energy_dataframe, x=energy_dataframe.index, y=power_pretty_names, color_discrete_sequence=power_colors, title='<b>DHW Usage and TM Load Ratios',
                    labels={'index': 'Data Point'}, 
                    height=400)
        
        num_data_points = len(df)
        x_shift = pd.Timedelta(hours=formatting_time_delta)  
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

        #ADD HPWH/SWING TANK OUTPUTS
        for col, color, name in zip(output_columns, output_colors, output_pretty_names):  
            stacked_fig.add_trace(
                go.Bar(
                x=x_positions_shifted,  
                y=output_dataframe[col],                
                name=name,                           
                marker_color=color                       
                )
                )
        

        return dcc.Graph(figure=stacked_fig)