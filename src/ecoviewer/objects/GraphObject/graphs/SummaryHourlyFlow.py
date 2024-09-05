from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.express as px
from ecoviewer.display.graphhelper import calc_daily_peakyness

class SummaryHourlyFlow(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Hourly Flow", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_hourly = dm.get_hourly_summary_data_df(self.summary_group)
        df_hourly['weekday'] = np.where(df_hourly.index.weekday <= 4, 1, 0)
        df_hourly['hour'] = df_hourly.index.hour
        df_hourly['date'] = df_hourly.index.date

        weekday = df_hourly.loc[df_hourly.weekday == 1]
        weekend = df_hourly.loc[df_hourly.weekday == 0]
        
        weekday = weekday.pivot(index = 'hour', columns = 'date', values = dm.flow_variable)
        weekend = weekend.pivot(index = 'hour', columns = 'date', values = dm.flow_variable)
        
        highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile = self.extract_percentile_days(dm, 0.98)
        
        fig = make_subplots(rows=1, cols=2, vertical_spacing = 0.025, horizontal_spacing = 0.025, shared_xaxes=False)
        
        for i in range(len(weekday.columns)):
            fig.add_trace(go.Scatter(
                x = weekday.index,
                y = weekday.iloc[:,i] * 60 / dm.occupant_capacity,
                name = i,
                opacity = 0.2,
                marker = dict(color = "grey"),
                showlegend = False),
                row = 1,
                col = 1)
            
        for i in range(len(weekend.columns)):
            fig.add_trace(go.Scatter(
                x = weekend.index,
                y = weekend.iloc[:,i] * 60 / dm.occupant_capacity,
                name = i,
                opacity = 0.2,
                marker = dict(color = "grey"),
                showlegend = False),
                row = 1,
                col = 2)
        
        fig.add_trace(go.Scatter(x = weekday.index, y = highVolWeekdayProfile / dm.occupant_capacity, name = 'Peak Volume', marker = dict(color = "darkblue")), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = weekday.index, y = highPeakWeekdayProfile / dm.occupant_capacity, name = 'Peak Norm', marker = dict(color = "darkred")), row = 1, col = 1)
        fig.add_trace(go.Scatter(x = weekend.index, y = highVolWeekendProfile / dm.occupant_capacity, name = 'Peak Volume', marker = dict(color = "darkblue"), showlegend = False), row = 1, col = 2)
        fig.add_trace(go.Scatter(x = weekend.index, y = highPeakWeekendProfile / dm.occupant_capacity, name = 'Peak Norm', marker = dict(color = "darkred"), showlegend = False), row = 1, col = 2)


        fig.update_layout(title = '<b>Hourly DHW Flow')
        fig.update_xaxes(title = '<b>Weekday', row = 1, col = 1)
        fig.update_xaxes(title = '<b>Weekend', row = 1, col = 2)
        fig.update_yaxes(title = '<b>Gallons', row = 1, col = 1)

        return dcc.Graph(figure=fig)
    
    def extract_percentile_days(self, dm : DataManager, percentile):

        full_daily_df = dm.get_daily_data_df()
        daily_df = full_daily_df[[dm.flow_variable]].copy()
        full_hourly_df = dm.get_hourly_flow_data_df()
        hourly_df = full_hourly_df[[dm.flow_variable]].copy()

        daily_df['weekday'] = np.where(daily_df.index.weekday <= 4, 1, 0)
        hourly_df['weekday'] = np.where(hourly_df.index.weekday <= 4, 1, 0)

        daily_df = calc_daily_peakyness(daily_df, hourly_df, flow_variable=dm.flow_variable)
        
        highVolWeekday = daily_df.loc[daily_df.weekday == 1, dm.flow_variable].quantile(percentile)
        highPeakWeekday = daily_df.loc[daily_df.weekday == 1, 'peak_norm'].quantile(percentile)
        
        highVolWeekend = daily_df.loc[daily_df.weekday == 0, dm.flow_variable].quantile(percentile)
        highPeakWeekend = daily_df.loc[daily_df.weekday == 0, 'peak_norm'].quantile(percentile)
        
        highVolWeekdayDate = daily_df.loc[(daily_df[dm.flow_variable] >= highVolWeekday) & (daily_df.weekday == 1), dm.flow_variable].idxmin().date() 
        highPeakWeekdayDate = daily_df.loc[(daily_df.peak_norm >= highPeakWeekday) & (daily_df.weekday == 1), 'peak_norm'].idxmin().date() 
        
        highVolWeekendDate = daily_df.loc[(daily_df[dm.flow_variable] >= highVolWeekend) & (daily_df.weekday == 0), dm.flow_variable].idxmin().date() 
        highPeakWeekendDate = daily_df.loc[(daily_df.peak_norm >= highPeakWeekend) & (daily_df.weekday == 0), 'peak_norm'].idxmin().date()

        highVolWeekdayProfile = hourly_df.loc[hourly_df.index.date == highVolWeekdayDate, dm.flow_variable] * 60 
        highPeakWeekdayProfile = hourly_df.loc[hourly_df.index.date == highPeakWeekdayDate, dm.flow_variable] * 60

        highVolWeekendProfile = hourly_df.loc[hourly_df.index.date == highVolWeekendDate, dm.flow_variable] * 60
        highPeakWeekendProfile = hourly_df.loc[hourly_df.index.date == highPeakWeekendDate, dm.flow_variable] * 60

        for series in [highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile]:
            series.index = series.index.hour

        return highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile