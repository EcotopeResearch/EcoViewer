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

def get_user_permissions_from_db(user_email : str, sql_dash_config):
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

def get_organized_mapping(df_columns, graph_df : pd.DataFrame, field_df : pd.DataFrame, selected_table : str):
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