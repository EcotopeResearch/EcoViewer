import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots
import plotly.colors
import numpy as np
import math
from ecoviewer.config import get_organized_mapping, round_df_to_3_decimal
from datetime import datetime
from datetime import time
#import statsmodels.api as sm
from .graphhelper import query_daily_flow_percentiles, calc_daily_peakyness, extract_percentile_days, query_daily_data, query_hourly_data

state_colors = {
    "loadUp" : "green",
    "shed" : "blue"}

def get_state_colors():
    return state_colors

def update_graph_time_frame(value, start_date, end_date, df, unit):
    dff = pd.DataFrame()
    if not isinstance(value, list):
        value = [value]
    if start_date != None and end_date != None:
        dff = df.loc[start_date:end_date, value]
    else:
        dff = df[value]
    fig = px.line(dff, x=dff.index, y=dff.columns)
    fig.update_layout(xaxis_title = 'Timestamp', yaxis_title = unit)
    return fig

def create_date_note(site_name, cursor, pretty_name):
    """
    returns [date_note, first_date, last_date]
    """
    query = f"SELECT time_pt FROM {site_name} ORDER BY time_pt ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 0 or len(result[0]) == 0:
        return ""
    first_date = result[0][0]

    query = f"SELECT time_pt FROM {site_name} ORDER BY time_pt DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    last_date = result[0][0]

    return [
            f"Possible range for {pretty_name}:",
            html.Br(),
            f"{first_date.strftime('%m/%d/%y')} - {last_date.strftime('%m/%d/%y')}"
    ]

def clean_df(df : pd.DataFrame, organized_mapping):
    for key, value in organized_mapping.items():
        fields = value["y1_fields"] + value["y2_fields"]

        # Iterate over the values and add traces to the figure
        for field_dict in fields:
            column_name = field_dict["column_name"]
            if 'lower_bound' in field_dict:
                df[column_name] = np.where(df[column_name] < field_dict["lower_bound"], np.nan, df[column_name])

            if 'upper_bound' in field_dict:
                df[column_name] = np.where(df[column_name] > field_dict["upper_bound"], np.nan, df[column_name])

def create_conjoined_graphs(df : pd.DataFrame, organized_mapping, add_state_shading : bool = False):
    clean_df(df, organized_mapping)
    graph_components = []
    # Load the JSON data from the file
    subplot_titles = []
    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = value["title"]
        subplot_titles.append(f"<b>{category}</b>")
    # Create a new figure for the category
    fig = make_subplots(rows = len(organized_mapping.items()), cols = 1, 
                specs=[[{"secondary_y": True}]]*len(organized_mapping.items()),
                shared_xaxes=True,
                vertical_spacing = 0.1/max(1, len(organized_mapping.items())),
                subplot_titles = subplot_titles)
    
    row = 0
    cop_columns = []

    for key, value in organized_mapping.items():
        row += 1
        # Extract the category (e.g., Temperature or Power)
        category = value["title"]

        # Extract the y-axis units
        y1_units = value["y1_units"]
        y2_units = value["y2_units"]

        # Extract the values for the category
        y1_fields = value["y1_fields"]
        y2_fields = value["y2_fields"]

        # Iterate over the values and add traces to the figure
        for field_dict in y1_fields:
            name = field_dict["readable_name"]
            column_name = field_dict["column_name"]
            y_axis = 'y1'
            secondary_y = False
            fig.add_trace(
                go.Scatter(
                    x=df.index, 
                    y=df[column_name], 
                    name=name, 
                    yaxis=y_axis, 
                    mode='lines',
                    hovertemplate="<br>".join([
                        f"{name}",
                        "time_pt=%{x}",
                        "value=%{y}",
                    ])
                ), 
                row=row, 
                col = 1, 
                secondary_y=secondary_y)
        for field_dict in y2_fields:
            name = field_dict["readable_name"]
            column_name = field_dict["column_name"]
            if 'COP' in column_name:
                cop_columns.append(column_name)
            y_axis = 'y2'
            secondary_y = True
            fig.add_trace(
                go.Scatter(
                    x=df.index, 
                    y=df[column_name], 
                    name=name, 
                    yaxis=y_axis, 
                    mode='lines',
                    hovertemplate="<br>".join([
                        f"{name}",
                        "time_pt=%{x}",
                        "value=%{y}",
                    ])
                ), 
                row=row, 
                col = 1, 
                secondary_y=secondary_y)

        fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row, col = 1, secondary_y = False)
        fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row, col = 1, secondary_y = True)

    fig.update_xaxes(title_text="<b>Time</b>", row = row, col = 1)
    fig.update_layout(
        width=1500,
        height=len(organized_mapping.items())*350)

    # shading for system_state
    if add_state_shading and "system_state" in df.columns:
        y1_height = df[cop_columns].max().max() + 0.25
        y1_base = df[cop_columns].min().min() - 0.25
        # Create a boolean mask to identify the start of a new block
        df['system_state'].fillna('normal', inplace=True)
        state_change = df['system_state'] != df['system_state'].shift(1)

        # Use the boolean mask to find the start indices of each block
        state_change_indices = df.index[state_change].tolist()
        for i in range(len(state_change_indices)-1):
            change_time = state_change_indices[i]
            system_state = df.at[change_time, 'system_state']
            if system_state != 'normal':
                fig.add_shape(
                    type="rect",
                    yref="y4",
                    x0=change_time,
                    y0=y1_base,
                    x1=state_change_indices[i+1],
                    y1=y1_height,
                    fillcolor=state_colors[system_state],
                    opacity=0.2,
                    line=dict(width=0)
                )

        # Add the final vrect if needed
        if len(state_change_indices) > 0 and df.at[state_change_indices[-1], 'system_state'] != 'normal':
            system_state = df.at[state_change_indices[-1], 'system_state']
            fig.add_shape(
                        type="rect",
                        yref="y2",
                        x0=state_change_indices[-1],
                        y0=0,
                        x1=df.index[-1], # large value to represent end of graph
                        y1=100,
                        fillcolor=state_colors[system_state],
                        opacity=0.2,
                        line=dict(width=0)
                    )

    figure = go.Figure(fig)

    # Add the figure to the array of graph objects
    graph_components.append(dcc.Graph(figure=figure))
    return graph_components

def _format_x_axis_date_str(dt_1 : datetime, dt_2 : datetime = None) -> str:
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

def _create_summary_bar_graph(og_df : pd.DataFrame):
    # Filter columns with the prefix "PowerIn_" and exclude "PowerIn_Total"
    powerin_columns = [col for col in og_df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and og_df[col].dtype == "float64"]
    cop_columns = [col for col in og_df.columns if 'COP' in col]
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
        df = round_df_to_3_decimal(df)

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
            x_axis_tick_text.append(_format_x_axis_date_str(first_date, last_date))
            x_val += pd.Timedelta(weeks=1)
        else:
            x_axis_tick_text.append(_format_x_axis_date_str(x_val))
            x_val += pd.Timedelta(days=1)

    energy_dataframe = df[powerin_columns].copy()
    # Multiply all values in the specified columns by 24
    energy_dataframe[powerin_columns] = energy_dataframe[powerin_columns].apply(lambda x: x * 24)

    # TODO error for no power columns


    # Create a stacked bar graph using Plotly Express
    stacked_fig = px.bar(energy_dataframe, x=energy_dataframe.index, y=powerin_columns, title='Energy and COP',
                labels={'index': 'Data Point'}, height=400)
    
    num_data_points = len(df)
    x_shift = pd.Timedelta(hours=formatting_time_delta)  # Adjust this value to control the horizontal spacing between the bars
    x_positions_shifted = [x + x_shift for x in df.index]
    # create fake bar for spacing
    stacked_fig.add_trace(go.Bar(x=x_positions_shifted, y=[0]*num_data_points, showlegend=False))
    stacked_fig.update_layout(
        # width=1300,
        yaxis1=dict(
            title='Avg. Daily kWh' if compress_to_weeks else 'kWh',
        ),
        xaxis=dict(
            title='Week' if compress_to_weeks else 'Day',
            tickmode = 'array',
            tickvals = x_axis_tick_val,
            ticktext = x_axis_tick_text  
        ),
        margin=dict(l=10, r=10),
        legend=dict(x=1.1)
    )

    # Add the additional columns as separate bars next to the stacks
    if len(cop_columns) > 0:
        for col in cop_columns:
            x_positions_shifted = [x + x_shift for x in df.index]
            stacked_fig.add_trace(go.Bar(
                x=x_positions_shifted, 
                y=df[col], 
                name=col, 
                yaxis = 'y2',
                customdata=np.transpose([x_axis_tick_text, [col]*len(x_axis_tick_text)]),
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
                title='COP',
                overlaying='y',
                side='right'
            ),
        )

    return dcc.Graph(figure=stacked_fig)

def _create_summary_Hourly_graph(df : pd.DataFrame, hourly_df : pd.DataFrame):
    powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and df[col].dtype == "float64"]

    nls_df = hourly_df[hourly_df['load_shift_day'] == 0]
    ls_df = hourly_df[hourly_df['load_shift_day'] == 1]

    ls_df = ls_df.groupby('hr').mean(numeric_only = True)
    ls_df = round_df_to_3_decimal(ls_df)

    nls_df = nls_df.groupby('hr').mean(numeric_only = True)
    nls_df = round_df_to_3_decimal(nls_df)

    power_df = hourly_df.groupby('hr').mean(numeric_only = True)
    power_df = round_df_to_3_decimal(power_df)

    power_fig = px.line(title = "Average Daily Power")
    
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
            title='kW',
        ),
        xaxis=dict(
            title='Hour',
        ),
        legend=dict(x=1.2),
        margin=dict(l=10, r=10),
    )
    
    return dcc.Graph(figure=power_fig)

def _create_summary_pie_graph(df):
    powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
    sums = df[powerin_columns].sum()
    pie_fig = px.pie(names=sums.index, values=sums.values, title='Distribution of Energy')
    return dcc.Graph(figure=pie_fig)

def _create_summary_gpdpp_timeseries(site_df_row, cursor):

    df_daily = query_daily_data(site_df_row.daily_table, cursor)

    df_daily['Flow_CityWater_Total'] = df_daily['Flow_CityWater'] * (60 * 24) #average GPM * 60min/hr * 24hr/day
    df_daily['Flow_CityWater_PP'] = round(df_daily['Flow_CityWater_Total'] / site_df_row.occupant_capacity, 2)

    mean_daily_usage, high_daily_usage = query_daily_flow_percentiles(site_df_row.daily_table, 0.95, cursor) / site_df_row.occupant_capacity 

    fig = go.Figure()
    fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily.Flow_CityWater_PP, mode = 'markers',
                             marker = dict(size=5, color = 'darkblue'), showlegend=False))
    
    fig.add_trace(go.Scatter(
    x=[df_daily.index.min() - pd.Timedelta(hours = 23), df_daily.index.max() + pd.Timedelta(hours = 23)],
    y=[mean_daily_usage, mean_daily_usage],
    mode="lines",
    line=dict(color="darkred", dash="dash"),
    name="Mean Daily Usage",
    hoverinfo="text",
    hovertext=f"Mean Daily Usage: {mean_daily_usage:.2f} Gallons/Person/Day"))
    
    fig.add_trace(go.Scatter(
    x=[df_daily.index.min() - pd.Timedelta(hours = 23), df_daily.index.max() + pd.Timedelta(hours = 23)],
    y=[high_daily_usage, high_daily_usage],
    mode="lines",
    line=dict(color="darkgreen", dash="dash"),
    name="95th Percentile Usage",
    hoverinfo="text",
    hovertext=f"95th Percentile Usage: {high_daily_usage:.2f} Gallons/Person/Day"))

    fig.update_layout(title = '<b>Daily Hot Water Usage')
    fig.update_yaxes(title = '<b>Gallons/Person/Day')
    fig.update_xaxes(title = '<b>Time')

    return dcc.Graph(figure=fig)


def _create_summary_peak_norm(df_hourly, df_daily, site_df_row):
    
    df_daily_with_peak = calc_daily_peakyness(df_daily, df_hourly)
    
    df_daily_with_peak['Flow_CityWater_PP'] = df_daily_with_peak['Flow_CityWater']  * 60 * 24 / site_df_row.occupant_capacity

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_daily_with_peak['Flow_CityWater_PP'], y=df_daily_with_peak['peak_norm'], mode='markers', marker=dict(color='darkblue')))
    fig.update_layout(title = '<b>Daily Peak Norm')
    fig.update_yaxes(title = '<b>Daily Max Fraction of DHW Consumed in 3-Hour Period')
    fig.update_xaxes(title = '<b>Gallons/Person/Day')

    return dcc.Graph(figure=fig)


def _create_summary_hourly_flow(df_hourly, df_daily, site_df_row, cursor):

    df_hourly['weekday'] = np.where(df_hourly.index.weekday <= 4, 1, 0)
    df_hourly['hour'] = df_hourly.index.hour
    df_hourly['date'] = df_hourly.index.date

    weekday = df_hourly.loc[df_hourly.weekday == 1]
    weekend = df_hourly.loc[df_hourly.weekday == 0]
    
    weekday = weekday.pivot(index = 'hour', columns = 'date', values = 'Flow_CityWater')
    weekend = weekend.pivot(index = 'hour', columns = 'date', values = 'Flow_CityWater')
    
    highVolWeekdayProfile, highPeakWeekdayProfile, highVolWeekendProfile, highPeakWeekendProfile = extract_percentile_days(site_df_row.daily_table, 0.98, cursor, site_df_row.hour_table)
    
    fig = make_subplots(rows=1, cols=2, vertical_spacing = 0.025, horizontal_spacing = 0.025, shared_xaxes=False)
    
    for i in range(len(weekday.columns)):
        fig.add_trace(go.Scatter(
            x = weekday.index,
            y = weekday.iloc[:,i] * 60 / site_df_row.occupant_capacity,
            name = i,
            opacity = 0.2,
            marker = dict(color = "grey"),
            showlegend = False),
            row = 1,
            col = 1)
        
    for i in range(len(weekend.columns)):
        fig.add_trace(go.Scatter(
            x = weekend.index,
            y = weekend.iloc[:,i] * 60 / site_df_row.occupant_capacity,
            name = i,
            opacity = 0.2,
            marker = dict(color = "grey"),
            showlegend = False),
            row = 1,
            col = 2)
    
    fig.add_trace(go.Scatter(x = weekday.index, y = highVolWeekdayProfile / site_df_row.occupant_capacity, name = 'Peak Volume', marker = dict(color = "darkblue")), row = 1, col = 1)
    fig.add_trace(go.Scatter(x = weekday.index, y = highPeakWeekdayProfile / site_df_row.occupant_capacity, name = 'Peak Norm', marker = dict(color = "darkred")), row = 1, col = 1)
    fig.add_trace(go.Scatter(x = weekend.index, y = highVolWeekendProfile / site_df_row.occupant_capacity, name = 'Peak Volume', marker = dict(color = "darkblue"), showlegend = False), row = 1, col = 2)
    fig.add_trace(go.Scatter(x = weekend.index, y = highPeakWeekendProfile / site_df_row.occupant_capacity, name = 'Peak Norm', marker = dict(color = "darkred"), showlegend = False), row = 1, col = 2)


    fig.update_layout(title = '<b>Hourly DHW Flow')
    fig.update_xaxes(title = '<b>Weekday', row = 1, col = 1)
    fig.update_xaxes(title = '<b>Weekend', row = 1, col = 2)
    fig.update_yaxes(title = '<b>Gallons', row = 1, col = 1)

    return dcc.Graph(figure=fig)


def _create_summary_cop_regression(site_df_row, cursor):
    
    df_daily = query_daily_data(site_df_row.daily_table, cursor)
    
    df_daily['Temp_OutdoorAir'] = df_daily['Temp_OAT']
    df_daily['SystemCOP'] = df_daily['COP_BoundaryMethod']

    fig = px.scatter(df_daily, x='Temp_OutdoorAir', y='SystemCOP',
                 title='<b>Outdoor Air Temperature & System COP Regression', trendline="ols", 
                 labels={'Temp_OutdoorAir': '<b>Outdoor Air Temperature', 'SystemCOP': '<b>System COP', 
                         'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
    color_discrete_sequence=["darkblue"]
                         )
    

    return dcc.Graph(figure=fig)

def _create_summary_cop_timeseries(site_df_row, cursor):

    df_daily = query_daily_data(site_df_row.daily_table, cursor)

    df_daily['SystemCOP'] = df_daily['COP_BoundaryMethod']
    df_daily['Temp_OutdoorAir'] = df_daily['Temp_OAT']

    fig = make_subplots(specs = [[{'secondary_y':True}]])
    fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily.SystemCOP,
                             mode = 'markers', name = 'System COP',
                             marker=dict(color='darkred')), secondary_y = True)
    
    fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily.Temp_OutdoorAir,
                             mode = 'markers', name = 'Outdoor Air Temerature',
                              marker=dict(color='darkgreen')), secondary_y = False)
    
    fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily.Temp_CityWater,
                             mode = 'markers', name = 'City Water Temperature',
                            marker=dict(color='darkblue')), secondary_y = False)

    fig.update_layout(title = '<b>System COP')
    fig.update_xaxes(title = '<b>Date')
    fig.update_yaxes(title = '<b>System COP', secondary_y = True)
    fig.update_yaxes(title = '<b>Daily Average Air and Water Temperature (F)', secondary_y = False)

    return dcc.Graph(figure=fig)

def _create_summary_boxwhisker_flow(cursor, site_df_row):

    hourly_df = query_hourly_data(site_df_row.hour_table, cursor)
    hourly_df['hour'] = hourly_df.index.hour
    hourly_df['Flow_CityWater_PerTenant'] = hourly_df['Flow_CityWater'] * 60 / site_df_row.occupant_capacity

    fig = px.box(hourly_df, x = 'hour', y = 'Flow_CityWater_PerTenant', color_discrete_sequence=['darkblue'])
    fig.update_layout(title = '<b>Hourly DHW Usage')
    fig.update_xaxes(title = '<b>Hour')
    fig.update_yaxes(title = '<b>Gallons/Tenant')



    return dcc.Graph(figure=fig)

# Define a function to check if a value is numeric
def _is_numeric(value):
    return pd.api.types.is_numeric_dtype(pd.Series([value]))

def _create_summary_gpdpp_histogram(df_daily : pd.DataFrame, site_df_row):
    if pd.notna(site_df_row['occupant_capacity']) and _is_numeric(site_df_row['occupant_capacity']) and 'Flow_CityWater' in df_daily.columns:
        nTenants = site_df_row['occupant_capacity'] # TODO get this from central site csv
        df_daily['DHWDemand'] = df_daily['Flow_CityWater']*60*24/nTenants
        fig = px.histogram(df_daily, x='DHWDemand', title='Domestic Hot Water Demand (' + str(int(nTenants)) + ' Tenants)',
                        labels={'DHWDemand': 'Gallons/Person/Day'})
        return dcc.Graph(figure=fig)
    else:
        if not (pd.notna(site_df_row['occupant_capacity']) and _is_numeric(site_df_row['occupant_capacity'])):
            error_msg = "erroneous occupant_capacity in site configuration."
        else:
            error_msg = "daily dataframe missing 'Flow_CityWater'."
        return html.P(style={'color': 'red'}, children=[
                    f"Error: could not load GPDPP histogram due to {error_msg}"
                ])
    
def _create_summary_erv_performance(df : pd.DataFrame):
    
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


def _create_summary_ohp_performance(df : pd.DataFrame):

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

def _create_summary_SERA_pie(df):

    power_cols = ['PowerIn_Lighting', 'PowerIn_PlugsMisc', 'PowerIn_Ventilation', 'PowerIn_HeatingCooling', 'PowerIn_DHW']
    power_data = df[power_cols].mean()
    colors = px.colors.qualitative.Antique

    fig = px.pie(names = power_data.index, values = power_data.values, title = '<b>Energy Consumption',
                 color_discrete_sequence = [colors[1], colors[2], colors[3], colors[4], colors[5]])

    return dcc.Graph(figure=fig)

def create_summary_graphs(daily_df, hourly_df, config_df, site_df_row, cursor):

    graph_components = []
    
    filtered_df = config_df[config_df['summary_group'].notna()]

    unique_groups = filtered_df['summary_group'].unique()
    for unique_group in unique_groups:
        filtered_group_df = config_df[config_df['summary_group']==unique_group]
        group_columns = [col for col in daily_df.columns if col in filtered_group_df['field_name'].tolist()]
        group_df = daily_df[group_columns]
        # Title if multiple groups:
        if len(unique_groups) > 1:
            graph_components.append(html.H2(unique_group))
        # Bar Graph
        if site_df_row["summary_bar_graph"]:
            graph_components.append(_create_summary_bar_graph(group_df))
        # Hourly Power Graph
        if site_df_row["summary_hour_graph"]:
            graph_components.append(_create_summary_Hourly_graph(group_df,hourly_df))
        # Pie Graph
        if site_df_row["summary_pie_chart"]:
            graph_components.append(_create_summary_pie_graph(group_df))
        if site_df_row["summary_gpdpp_histogram"]:
            graph_components.append(_create_summary_gpdpp_histogram(group_df, site_df_row))
        # GPDPP Timeseries
        if site_df_row['summary_gpdpp_timeseries']:
            graph_components.append(_create_summary_gpdpp_timeseries(site_df_row, cursor))
        # Peak Norm Scatter
        if site_df_row['summary_peaknorm']:
            graph_components.append(_create_summary_peak_norm(hourly_df, group_df, site_df_row))
        # Hourly Flow Percentiles
        if site_df_row['summary_hourly_flow']:
            graph_components.append(_create_summary_hourly_flow(hourly_df, group_df, site_df_row, cursor))
        # COP Regression
        if site_df_row['summary_cop_regression']:
            graph_components.append(_create_summary_cop_regression(site_df_row, cursor))
        # COP Timeseries
        if site_df_row['summary_cop_timeseries']:
            graph_components.append(_create_summary_cop_timeseries(site_df_row, cursor))
        # DHW Box and Whisker
        if site_df_row['summary_flow_boxwhisker']:
            graph_components.append(_create_summary_boxwhisker_flow(cursor, site_df_row))
        
        # ERV active vs passive hourly profile
        if site_df_row['summary_erv_performance']:
            graph_components.append(_create_summary_erv_performance(group_df))

        if site_df_row['summary_ohp_performance']:
            graph_components.append(_create_summary_ohp_performance(group_df))

        # SERA office summary
        if site_df_row['summary_SERA_pie']:
            graph_components.append(_create_summary_SERA_pie(group_df))

    return graph_components

def create_hourly_shapes(df : pd.DataFrame, graph_df : pd.DataFrame, field_df : pd.DataFrame, selected_table : str):
    hourly_only_field_df = field_df
    if 'hourly_shapes_display' in field_df.columns:
        hourly_only_field_df = field_df[field_df['hourly_shapes_display'] == True]
    organized_mapping = get_organized_mapping(df.columns, graph_df, hourly_only_field_df, selected_table)
    graph_components = []
    weekday_df = df[df['weekday'] == True]
    weekend_df = df[df['weekday'] == False]
    weekday_df = weekday_df.groupby('hr').mean(numeric_only = True)
    weekday_df = round_df_to_3_decimal(weekday_df)
    weekend_df = weekend_df.groupby('hr').mean(numeric_only = True)
    weekend_df = round_df_to_3_decimal(weekend_df)
    subplot_titles = []
    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = value["title"]
        subplot_titles.append(f"<b>{category} weekday</b>")
        subplot_titles.append(f"<b>{category} weekend</b>")

    
    # Create a new figure for the category
    fig = make_subplots(rows = len(organized_mapping.items())*2, cols = 1, 
                specs=[[{"secondary_y": True}]]*len(organized_mapping.items())*2,
                shared_xaxes=True,
                vertical_spacing = 0.1/max(1, len(organized_mapping.items())),
                subplot_titles = subplot_titles)
    
    row = 1
    colors = plotly.colors.DEFAULT_PLOTLY_COLORS
    color_num = 0

    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = value["title"]

        # Extract the y-axis units
        y1_units = value["y1_units"]
        y2_units = value["y2_units"]

        # Extract the values for the category
        y1_fields = value["y1_fields"]
        y2_fields = value["y2_fields"]

        # Iterate over the values and add traces to the figure
        y_axis = 'y1'
        secondary_y = False
        for field_dict in y1_fields:
            name = field_dict["readable_name"]
            column_name = field_dict["column_name"]
            if column_name in weekday_df.columns:
                weekday_trace = go.Scatter(x=weekday_df.index, y=weekday_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                            hovertemplate="<br>".join([
                                                f"{name}",
                                                "hour=%{x}",
                                                "value=%{y}",
                                            ]), 
                                            line=dict(color = colors[color_num]))
                weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines', 
                                            hovertemplate="<br>".join([
                                                f"{name}",
                                                "hour=%{x}",
                                                "value=%{y}",
                                            ]), 
                                            showlegend=False, line=dict(color = colors[color_num]))
                fig.add_trace(weekday_trace, row=row, col = 1, secondary_y=secondary_y)
                fig.add_trace(weekend_trace, row=row+1, col = 1, secondary_y=secondary_y)
                color_num += 1
                color_num = color_num % len(colors)

        y_axis = 'y2'
        secondary_y = True
        for field_dict in y2_fields:
            name = field_dict["readable_name"]
            column_name = field_dict["column_name"]
            if column_name in weekday_df.columns:
                weekday_trace = go.Scatter(x=weekday_df.index, y=weekday_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                            hovertemplate="<br>".join([
                                                f"{name}",
                                                "hour=%{x}",
                                                "value=%{y}",
                                            ]),
                                            line=dict(color = colors[color_num]))
                weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                            hovertemplate="<br>".join([
                                                f"{name}",
                                                "hour=%{x}",
                                                "value=%{y}",
                                            ]), 
                                            showlegend=False, line=dict(color = colors[color_num]))
                fig.add_trace(weekday_trace, row=row, col = 1, secondary_y=secondary_y)
                fig.add_trace(weekend_trace, row=row+1, col = 1, secondary_y=secondary_y)
                color_num += 1
                color_num = color_num % len(colors)

        fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row, col = 1, secondary_y = False)
        fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row, col = 1, secondary_y = True)
        fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row+1, col = 1, secondary_y = False)
        fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row+1, col = 1, secondary_y = True)

        row += 2

    fig.update_xaxes(title_text="<b>Hour</b>", row = row, col = 1)
    fig.update_layout(
        width=1300,
        height=len(organized_mapping.items())*460)

    figure = go.Figure(fig)
    # Add the figure to the array of graph objects
    graph_components.append(dcc.Graph(figure=figure))

    return graph_components

def bayview_power_processing(df : pd.DataFrame) -> pd.DataFrame:
    df['PowerIn_SwingTank'] = df['PowerIn_ERTank1'] + df['PowerIn_ERTank2'] + df['PowerIn_ERTank5'] + df['PowerIn_ERTank6']

    # Drop the 'PowerIn_ER#' columns
    df = df.drop(['PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6'], axis=1)
    return df

def bayview_prune_additional_power(df : pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = ['PowerIn_Swing', 'PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6', 'PowerIn_HPWH']
    columns_to_drop = [col for col in df.columns if col.startswith("PowerIn_") and col not in columns_to_keep]
    df = df.drop(columns=columns_to_drop)
    return df
    

