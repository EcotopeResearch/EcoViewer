import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots
import plotly.colors
import numpy as np
import math
#from ecoviewer.config import get_organized_mapping, round_df_to_3_decimal
from datetime import datetime
import mysql.connector


def get_summary_error_msg(e : Exception, summary_graph_name : str = "summary graph"):
    return html.P(style={'color': 'red', 'textAlign': 'center'}, children=[
                                        html.Br(),
                                        f"Could not generate {summary_graph_name}: {str(e)}"
                                    ])

def query_daily_flow_percentiles(daily_table, percentile, cursor, site_name):

    query = f"SELECT time_pt, Flow_CityWater FROM {daily_table};"
    cursor.execute(query)
    daily_df = cursor.fetchall()
    daily_df = pd.DataFrame(daily_df, columns = cursor.column_names)
    daily_df.set_index('time_pt', inplace = True)
    daily_df = apply_event_filters_to_df(daily_df,site_name,['HW_LOSS'],cursor)

    mean_day = daily_df['Flow_CityWater'].mean() * 24 * 60
    percentile_day = daily_df['Flow_CityWater'].quantile(percentile) * 24 * 60

    #cursor.close()

    return mean_day, percentile_day

def calc_daily_peakyness(daily_table, hourly_table, flow_variable : str = 'Flow_CityWater'):

    peak_volumes = []
    peak_hours = []
    peak_norm = []
    
    for i in range(0, len(daily_table)):    
        # slice dataframe on date
        date_str = str(daily_table.index[i])
        
        next_day = str(daily_table.index[i] + pd.DateOffset(days=1))
        day = hourly_table[date_str:next_day]
     
        # find peak in that day
        peak = 0
        hr = 0
        for j in range(1,len(day)-1):

            # caluclate volumes for hours
            hr0 = day[flow_variable][j-1] # volume at previous hour
            hr1 = day[flow_variable][j] # volume at hour
            hr2 = day[flow_variable][j+1] # volume at furture hour
            
            # potential new peak
            new = hr0 + hr1 + hr2
            
            if new > peak:
                peak = new
                hr = j
            
        peak_volumes.append(peak)
        peak_hours.append(hr)
        peak_norm.append(peak / 24 /daily_table[flow_variable][i])    

    daily_table['peak_volumes'] = peak_volumes
    daily_table['peak_volumes'] = daily_table['peak_volumes']*60 # convert to gallons
    daily_table['peak_hours'] = peak_hours
    daily_table['peak_norm'] = peak_norm
    
    return daily_table

def extract_percentile_days(daily_table, percentile, cursor, hourly_table):

    query = f"SELECT time_pt, Flow_CityWater FROM {daily_table}"
    cursor.execute(query)
    daily_df = cursor.fetchall()
    daily_df = pd.DataFrame(daily_df, columns = cursor.column_names)
    daily_df.set_index('time_pt', inplace = True)

    query = f"SELECT time_pt, Flow_CityWater FROM {hourly_table}"
    cursor.execute(query)
    hourly_df = cursor.fetchall()
    hourly_df = pd.DataFrame(hourly_df, columns = cursor.column_names)
    hourly_df.set_index('time_pt', inplace = True)

    daily_df['weekday'] = np.where(daily_df.index.weekday <= 4, 1, 0)
    hourly_df['weekday'] = np.where(hourly_df.index.weekday <= 4, 1, 0)

    daily_df = calc_daily_peakyness(daily_df, hourly_df)
    
    highVolWeekday = daily_df.loc[daily_df.weekday == 1, 'Flow_CityWater'].quantile(percentile)
    highPeakWeekday = daily_df.loc[daily_df.weekday == 1, 'peak_norm'].quantile(percentile)
    
    highVolWeekend = daily_df.loc[daily_df.weekday == 0, 'Flow_CityWater'].quantile(percentile)
    highPeakWeekend = daily_df.loc[daily_df.weekday == 0, 'peak_norm'].quantile(percentile)
    
    highVolWeekdayDate = daily_df.loc[(daily_df.Flow_CityWater >= highVolWeekday) & (daily_df.weekday == 1), 'Flow_CityWater'].idxmin().date() 
    highPeakWeekdayDate = daily_df.loc[(daily_df.peak_norm >= highPeakWeekday) & (daily_df.weekday == 1), 'peak_norm'].idxmin().date() 
    
    highVolWeekendDate = daily_df.loc[(daily_df.Flow_CityWater >= highVolWeekend) & (daily_df.weekday == 0), 'Flow_CityWater'].idxmin().date() 
    highPeakWeekendDate = daily_df.loc[(daily_df.peak_norm >= highPeakWeekend) & (daily_df.weekday == 0), 'peak_norm'].idxmin().date()

    highVolWeekdayProfile = hourly_df.loc[hourly_df.index.date == highVolWeekdayDate, 'Flow_CityWater'] * 60 
    highPeakWeekdayProfile = hourly_df.loc[hourly_df.index.date == highPeakWeekdayDate, 'Flow_CityWater'] * 60

    highVolWeekendProfile = hourly_df.loc[hourly_df.index.date == highVolWeekendDate, 'Flow_CityWater'] * 60
    highPeakWeekendProfile = hourly_df.loc[hourly_df.index.date == highPeakWeekendDate, 'Flow_CityWater'] * 60

    for series in [highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile]:
        series.index = series.index.hour

   # return highVolWeekdayDate, highPeakWeekdayDate, highVolWeekendDate, highPeakWeekendDate
    return highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile

def apply_event_filters_to_df(df : pd.DataFrame, site_name : str, events_to_filter : list, cursor):
    query = f"SELECT start_time_pt, end_time_pt FROM site_events WHERE site_name = '{site_name}' AND event_type IN ("
    if len(events_to_filter) > 0:
        query = f"{query}'{events_to_filter[0]}'"
        for event_type in events_to_filter[1:]:
            query = f"{query},'{event_type}'"
    query = f"{query});"

    cursor.execute(query)
    time_ranges = cursor.fetchall()

    time_ranges = [(pd.to_datetime(start_time), pd.to_datetime(end_time)) for start_time, end_time in time_ranges]

    # Remove points in the DataFrame whose indexes fall within the time ranges
    for start_time, end_time in time_ranges:
        df = df.loc[~((df.index >= start_time) & (df.index <= end_time))]

    return df

def query_daily_data(daily_table, cursor):

    query = f"SELECT * FROM {daily_table};"
    cursor.execute(query)
    daily_df = cursor.fetchall()
    daily_df = pd.DataFrame(daily_df, columns = cursor.column_names)
    daily_df.set_index('time_pt', inplace = True)

    return daily_df

def query_hourly_data(hourly_table, cursor):

    query = f"SELECT * FROM {hourly_table};"
    cursor.execute(query)
    hourly_df = cursor.fetchall()
    hourly_df = pd.DataFrame(hourly_df, columns = cursor.column_names)
    hourly_df.set_index('time_pt', inplace = True)

    return hourly_df

def query_annual_data(table, cursor):

    query = f"SELECT * FROM {table};"
    cursor.execute(query)
    annual_df = cursor.fetchall()
    annual_df = pd.DataFrame(annual_df, columns = cursor.column_names)
    annual_df.set_index('time_pt', inplace = True)

    last_day = annual_df.index.max()
    first_day = last_day - pd.DateOffset(years=1) + pd.DateOffset(days=1)
    
    annual_df = annual_df.loc[(annual_df.index >= first_day) & (annual_df.index <= last_day)]
    return annual_df, first_day.strftime('%m/%d/%Y'), last_day.strftime('%m/%d/%Y')