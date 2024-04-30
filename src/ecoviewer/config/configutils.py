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

def get_user_permissions_from_db(user_email : str, sql_dash_config, exclude_csv_only_fields : bool = True):
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

        field_query = """
            SELECT * FROM field
            WHERE site_name IN ({})
        """.format(', '.join(['%s'] * len(table_names)))
        if exclude_csv_only_fields:
            field_query = f"{field_query} AND graph_id IS NOT NULL" 
        
        cursor.execute(field_query, table_names)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        field_df = pd.DataFrame(result, columns=column_names)
        print("field df length", len(field_df.index))

    cursor.close()
    cnx.close()

    display_drop_down = []
    for name in table_names:
        display_drop_down.append({'label': site_df.loc[name, "pretty_name"], 'value' : name})
    return site_df, graph_df, field_df, display_drop_down

def get_organized_mapping(df_columns, graph_df : pd.DataFrame, field_df : pd.DataFrame, selected_table : str, all_fields : bool = False):
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
            if all_fields or field_name in df_columns:
                column_details = {}
                column_details["readable_name"] = field_row['pretty_name']
                column_details["column_name"] = field_name
                column_details["description"] = field_row["description"]
                # if not math.isnan(field_row["lower_bound"]):
                if field_row["lower_bound"] is not None and not math.isnan(field_row["lower_bound"]):
                    column_details["lower_bound"] = field_row["lower_bound"]
                # if not math.isnan(field_row["upper_bound"]):
                if field_row["upper_bound"] is not None and not math.isnan(field_row["upper_bound"]):
                    column_details["upper_bound"] = field_row["upper_bound"]
                secondary_y = field_row['secondary_y']
                if not secondary_y:
                    y1_fields.append(column_details)
                else:
                    y2_fields.append(column_details)
        if len(y1_fields) == 0:
            if len(y2_fields) > 0:
                returnDict[index] = {
                    "title" : row['graph_title'],
                    "y1_units" : y2_units,
                    "y2_units" : y1_units,
                    "y1_fields" : y2_fields,
                    "y2_fields" : y1_fields
                }
        else:
            returnDict[index] = {
                "title" : row['graph_title'],
                "y1_units" : y1_units,
                "y2_units" : y2_units,
                "y1_fields" : y1_fields,
                "y2_fields" : y2_fields
            }
    return returnDict

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

def parse_checklists_from_div(div_children : list) -> list:
    ret_list = []
    for element in div_children:
        if 'type' in element:
            if element['type'] == 'Checklist':
                ret_list = ret_list + element['props']['value']
            elif element['type'] == 'Div':
                ret_list = ret_list + parse_checklists_from_div(element['props']['children'])
    return ret_list

def get_df_from_query(query : str, cursor) -> pd.DataFrame:
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=column_names)

    # round float columns to 3 decimal places
    df = round_df_to_3_decimal(df)
    return df

def round_df_to_3_decimal(df : pd.DataFrame) -> pd.DataFrame:
    float_cols = df.select_dtypes(include=['float64'])
    df[float_cols.columns] = float_cols.round(3)
    return df