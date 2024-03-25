import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots
import plotly.colors
import mysql.connector
import math
import numpy as np
from datetime import datetime

state_colors = {
    "loadUp" : "green",
    "shed" : "blue"
}

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

def create_data_dictionary(organized_mapping):
    returnStr = [html.H2('Variable Definitions', style={'font-size': '24px'})]
    for key, value in organized_mapping.items():
        returnStr.append(html.H3(key, style={'font-size': '18px'}))
        y1_fields = value["y1_fields"]
        y2_fields = value["y2_fields"]
        for field_dict in y1_fields:
            name = field_dict["readable_name"]
            descr = field_dict["description"]
            if descr is None:
                descr = ''
            returnStr.append(html.P([
                html.Span(''+name+'',style={"font-weight": "bold"}),
                html.Span(' : '+descr,style={"font-weight": "normal"}),
                ],
                style={'font-size': '14px', 'text-indent': '40px'}
            ))
        for field_dict in y2_fields:
            name = field_dict["readable_name"]
            descr = field_dict["description"]
            returnStr.append(html.P([
                html.Span(''+name+'',style={"font-weight": "bold"}),
                html.Span(' : '+descr,style={"font-weight": "normal"}),
                ],
                style={'font-size': '14px', 'text-indent': '40px'}
            ))
    return returnStr

def get_organized_mapping(df_columns, graph_df, field_df, selected_table):
    returnDict = {}
    site_fields = field_df[field_df['site_name'] == selected_table]
    site_fields = site_fields.set_index('field_name')
    for index, row in graph_df.iterrows():
        # Extract the y-axis units
        y1_units = row["y_1_title"] if row["y_1_title"] != None else ""
        y2_units = row["y_2_title"] if row["y_2_title"] != None else ""
        y1_fields = []
        y2_fields = []
        for field_name, field_row in site_fields[site_fields['graph_id'] == index].iterrows():
            if field_name in df_columns:
                column_details = {}
                column_details["readable_name"] = field_row['pretty_name']
                column_details["column_name"] = field_name
                column_details["description"] = field_row["description"]
                if not math.isnan(field_row["lower_bound"]):
                # if not (field_row["lower_bound"] is None or not math.isnan(field_row["lower_bound"])):
                    column_details["lower_bound"] = field_row["lower_bound"]
                if not math.isnan(field_row["upper_bound"]):
                # if not (field_row["upper_bound"] is None or math.isnan(field_row["upper_bound"])):
                    column_details["upper_bound"] = field_row["upper_bound"]
                secondary_y = field_row['secondary_y']
                if not secondary_y:
                    y1_fields.append(column_details)
                else:
                    y2_fields.append(column_details)
        if len(y1_fields) == 0:
            if len(y2_fields) > 0:
                returnDict[row['graph_title']] = {
                    "y1_units" : y2_units,
                    "y2_units" : y1_units,
                    "y1_fields" : y2_fields,
                    "y2_fields" : y1_fields
                }
        else:
            returnDict[row['graph_title']] = {
                "y1_units" : y1_units,
                "y2_units" : y2_units,
                "y1_fields" : y1_fields,
                "y2_fields" : y2_fields
            }
    return returnDict

def create_date_note(site_name, cursor):
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
            f"Possible range for {site_name}:",
            html.Br(),
            f"{first_date.strftime('%m/%d/%y')} - {last_date.strftime('%m/%d/%y')}"
    ]

def clean_df(df, organized_mapping):
    for key, value in organized_mapping.items():
        fields = value["y1_fields"] + value["y2_fields"]

        # Iterate over the values and add traces to the figure
        for field_dict in fields:
            column_name = field_dict["column_name"]
            if 'lower_bound' in field_dict:
                df[column_name] = np.where(df[column_name] < field_dict["lower_bound"], np.nan, df[column_name])

            if 'upper_bound' in field_dict:
                df[column_name] = np.where(df[column_name] > field_dict["upper_bound"], np.nan, df[column_name])



def create_conjoined_graphs(df, organized_mapping, add_state_shading = False):
    clean_df(df, organized_mapping)
    graph_components = []
    # Load the JSON data from the file
    subplot_titles = []
    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = key
        subplot_titles.append(f"<b>{category}</b>")
    # Create a new figure for the category
    fig = make_subplots(rows = len(organized_mapping.items()), cols = 1, 
                specs=[[{"secondary_y": True}]]*len(organized_mapping.items()),
                shared_xaxes=True,
                vertical_spacing = 0.05,
                subplot_titles = subplot_titles)
    
    row = 0
    cop_columns = []

    for key, value in organized_mapping.items():
        row += 1
        # Extract the category (e.g., Temperature or Power)
        category = key

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
            fig.add_trace(go.Scatter(x=df.index, y=df[column_name], name=name, yaxis=y_axis, mode='lines'), row=row, col = 1, secondary_y=secondary_y)
        for field_dict in y2_fields:
            name = field_dict["readable_name"]
            column_name = field_dict["column_name"]
            if 'COP' in column_name:
                cop_columns.append(column_name)
            y_axis = 'y2'
            secondary_y = True
            fig.add_trace(go.Scatter(x=df.index, y=df[column_name], name=name, yaxis=y_axis, mode='lines'), row=row, col = 1, secondary_y=secondary_y)

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

def _create_summary_bar_graph(df):
    # Filter columns with the prefix "PowerIn_" and exclude "PowerIn_Total"
    # powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and col != 'PowerIn_Total' and df[col].dtype == "float64"]
    powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
    energy_dataframe = df[powerin_columns].copy()

    # Multiply all values in the specified columns by 24
    energy_dataframe[powerin_columns] = energy_dataframe[powerin_columns].apply(lambda x: x * 24)

    # TODO error for no power columns


    # Create a stacked bar graph using Plotly Express
    stacked_fig = px.bar(energy_dataframe, x=energy_dataframe.index, y=powerin_columns, title='Energy and COP',
                labels={'index': 'Data Point'}, height=400)
    
    num_data_points = len(df)
    x_shift = pd.Timedelta(hours=4)  # Adjust this value to control the horizontal spacing between the bars
    x_positions_shifted = [x + x_shift for x in df.index]
    # create fake bar for spacing
    stacked_fig.add_trace(go.Bar(x=x_positions_shifted, y=[0]*num_data_points, showlegend=False))
    stacked_fig.update_layout(
        # width=1300,
        yaxis1=dict(
            title='kWh',
        ),
        xaxis=dict(
            title='Day',
        ),
        margin=dict(l=10, r=10),
        legend=dict(x=1.1)
    )

    # Add the additional columns as separate bars next to the stacks
    cop_columns = [col for col in df.columns if 'COP' in col]
    if len(cop_columns) > 0:
        for col in cop_columns:
            x_positions_shifted = [x + x_shift for x in df.index]
            stacked_fig.add_trace(go.Bar(x=x_positions_shifted, y=df[col], name=col, yaxis = 'y2'))
            x_shift += pd.Timedelta(hours=4)
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

def _create_summary_Hourly_graph(df, hourly_df):
    powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and df[col].dtype == "float64"]

    nls_df = hourly_df[hourly_df['load_shift_day'] == 0]
    ls_df = hourly_df[hourly_df['load_shift_day'] == 1]

    ls_df = ls_df.groupby('hr').mean(numeric_only = True)
    nls_df = nls_df.groupby('hr').mean(numeric_only = True)
    power_df = hourly_df.groupby('hr').mean(numeric_only = True)
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

def create_summary_graphs(df, hourly_df, config_df):

    graph_components = []
    
    filtered_df = config_df[config_df['summary_group'].notna()]

    unique_groups = filtered_df['summary_group'].unique()
    for unique_group in unique_groups:
        filtered_group_df = config_df[config_df['summary_group']==unique_group]
        group_columns = [col for col in df.columns if col in filtered_group_df['field_name'].tolist()]
        
        group_df = df[group_columns]
        # Title if multiple groups:
        if len(unique_groups) > 1:
            graph_components.append(html.H2(unique_group))
        # Bar Graph
        graph_components.append(_create_summary_bar_graph(group_df))
        # Hourly Power Graph
        graph_components.append(_create_summary_Hourly_graph(group_df,hourly_df))
        # Pie Graph
        graph_components.append(_create_summary_pie_graph(group_df))

    return graph_components

def create_hourly_shapes(df, organized_mapping):

    graph_components = []
    weekday_df = df[df['weekday'] == True]
    weekend_df = df[df['weekday'] == False]
    weekday_df = weekday_df.groupby('hr').mean(numeric_only = True)
    weekend_df = weekend_df.groupby('hr').mean(numeric_only = True)
    subplot_titles = []
    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = key
        subplot_titles.append(f"<b>{category} weekday</b>")
        subplot_titles.append(f"<b>{category} weekend</b>")

    
    # Create a new figure for the category
    fig = make_subplots(rows = len(organized_mapping.items())*2, cols = 1, 
                specs=[[{"secondary_y": True}]]*len(organized_mapping.items())*2,
                shared_xaxes=True,
                vertical_spacing = 0.025,
                subplot_titles = subplot_titles)
    
    row = 1
    colors = plotly.colors.DEFAULT_PLOTLY_COLORS
    color_num = 0

    for key, value in organized_mapping.items():
        # Extract the category (e.g., Temperature or Power)
        category = key

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
                                            line=dict(color = colors[color_num]))
                weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines', 
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
                                            line=dict(color = colors[color_num]))
                weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines', 
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

def generate_summary_query(day_table, numDays = 7, start_date = None, end_date = None):
    summary_query = f"SELECT * FROM {day_table} "
    if start_date != None and end_date != None:
        summary_query += f"WHERE time_pt >= '{start_date}' AND time_pt <= '{end_date} 23:59:59' ORDER BY time_pt ASC"
    else:
        summary_query += f"ORDER BY time_pt DESC LIMIT {numDays}" #get last x days
        summary_query = f"SELECT * FROM ({summary_query}) AS subquery ORDER BY subquery.time_pt ASC;"
    return summary_query

def generate_hourly_summary_query(hour_table, day_table, numHours = 190, load_shift_tracking = True, start_date = None, end_date = None):
    if load_shift_tracking:
        hourly_summary_query = f"SELECT {hour_table}.*, HOUR({hour_table}.time_pt) AS hr, {day_table}.load_shift_day FROM {hour_table} " +\
            f"LEFT JOIN {day_table} ON {day_table}.time_pt = {hour_table}.time_pt "
    else:
        hourly_summary_query = f"SELECT {hour_table}.*, HOUR({hour_table}.time_pt) AS hr FROM {hour_table} "
    if start_date != None and end_date != None:
        hourly_summary_query += f"WHERE {hour_table}.time_pt >= '{start_date}' AND {hour_table}.time_pt <= '{end_date} 23:59:59' ORDER BY time_pt ASC"
    else:
        hourly_summary_query += f"ORDER BY {hour_table}.time_pt DESC LIMIT {numHours}" #get last 30 days plus some 740
        hourly_summary_query = f"SELECT * FROM ({hourly_summary_query}) AS subquery ORDER BY subquery.time_pt ASC;"

    return hourly_summary_query

def generate_raw_data_query(min_table, hour_table, day_table, field_df, selected_table, state_tracking = True, start_date = None, end_date = None):
    query = f"SELECT {min_table}.*, "
    if state_tracking:
        query += f"{hour_table}.system_state, "
    
    # conditionals because some sites don't have these
    if field_df[(field_df['field_name'] == 'OAT_NOAA') & (field_df['site_name'] == selected_table)].shape[0] > 0:
        query += f"{hour_table}.OAT_NOAA, "
    if field_df[(field_df['field_name'] == 'COP_Equipment') & (field_df['site_name'] == selected_table)].shape[0] > 0:
        query += f"{day_table}.COP_Equipment, "
    if field_df[(field_df['field_name'] == 'COP_DHWSys_2') & (field_df['site_name'] == selected_table)].shape[0] > 0:
        query += f"{day_table}.COP_DHWSys_2, "
    query += f"IF(DAYOFWEEK({min_table}.time_pt) IN (1, 7), FALSE, TRUE) AS weekday, " +\
        f"HOUR({min_table}.time_pt) AS hr FROM {min_table} "
    #TODO these two if statements are a work around for LBNLC. MAybe figure out better solution
    if min_table != hour_table:
        query += f"LEFT JOIN {hour_table} ON {min_table}.time_pt = {hour_table}.time_pt "
    if min_table != day_table:
        query += f"LEFT JOIN {day_table} ON {min_table}.time_pt = {day_table}.time_pt "

    if start_date != None and end_date != None:
        query += f"WHERE {min_table}.time_pt >= '{start_date}' AND {min_table}.time_pt <= '{end_date} 23:59:59' ORDER BY {min_table}.time_pt ASC"
    else:
        query += f"ORDER BY {min_table}.time_pt DESC LIMIT 4000"
        query = f"SELECT * FROM ({query}) AS subquery ORDER BY subquery.time_pt ASC;"

    return query

def get_user_permissions_from_db(user_email, sql_dash_config):
    email_groups = [user_email, user_email.split('@')[-1]]
    
    cnx = mysql.connector.connect(**sql_dash_config)
    cursor = cnx.cursor() 

    site_query = """
        SELECT *
        FROM site
        WHERE site_name IN
        (SELECT site_name from site_access WHERE user_group IN (
        SELECT user_group from user_groups WHERE email_address IN ({})
        ))
    """.format(', '.join(['%s'] * len(email_groups)))
    cursor.execute(site_query, email_groups)
    result = cursor.fetchall()
    if len(result) == 0:
        site_df, graph_df, field_df, table_names = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []
    else: 
        column_names = [desc[0] for desc in cursor.description]
        site_df = pd.DataFrame(result, columns=column_names)
        table_names = site_df["site_name"].values.tolist()
        site_df = site_df.set_index('site_name')

        cursor.execute("SELECT * FROM graph_display")
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        graph_df = pd.DataFrame(result, columns=column_names)
        graph_df = graph_df.set_index('graph_id')

        field_query = site_query = """
            SELECT * FROM field
            WHERE site_name IN ({})
        """.format(', '.join(['%s'] * len(table_names)))
        cursor.execute(field_query, table_names)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        field_df = pd.DataFrame(result, columns=column_names)

    cursor.close()
    cnx.close()

    display_drop_down = []
    for name in table_names:
        display_drop_down.append({'label': site_df.loc[name, "pretty_name"], 'value' : name})
    return site_df, graph_df, field_df, display_drop_down

def log_event(user_email, selected_table, start_date, end_date, sql_dash_config):
    cnx = mysql.connector.connect(**sql_dash_config)
    cursor = cnx.cursor() 

    fields = ['event_time', 'email_address']
    formated_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    values = [f'"{formated_date}"', f'"{user_email}"']

    if not selected_table is None:
        fields.append('site_name')
        values.append(f'"{selected_table}"')
    if not start_date is None:
        fields.append('start_date')
        values.append(f'"{start_date}"')
    if not end_date is None:
        fields.append('end_date')
        values.append(f'"{end_date}"')

    insert_query = f"INSERT INTO dash_activity_log ({', '.join(fields)}) VALUES ({', '.join(values)});"
    print(insert_query)

    cursor.execute(insert_query)
    
    # Commit the changes
    cnx.commit()
    cursor.close()
    cnx.close()

    
