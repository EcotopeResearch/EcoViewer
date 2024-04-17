import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Dash, dash_table
from plotly.subplots import make_subplots
import plotly.colors
import mysql.connector
import math
import numpy as np
from datetime import datetime

def create_meta_data_table(site_df : pd.DataFrame, selected_table : str, app : Dash):
    wh_unit_name = site_df.loc[selected_table, 'wh_unit_name']
    wh_manufacturer = site_df.loc[selected_table, 'wh_manufacturer']
    swing_tank_volume = site_df.loc[selected_table, 'swing_tank_volume']

    mapping = {
        "Address" : site_df.loc[selected_table, 'address'] if site_df.loc[selected_table, 'address'] is not None else "Unknown", 
        "Building Specifications" : site_df.loc[selected_table, 'building_specs'] if site_df.loc[selected_table, 'building_specs'] is not None else "Unknown", 
        "Primary System Model" : f"{wh_manufacturer} {wh_unit_name}" if not wh_manufacturer is None and not wh_unit_name is None else None, 
        "Primary HPWHs" : site_df.loc[selected_table, 'number_heat_pumps'], 
        "Primary Tank Volume" : site_df.loc[selected_table, 'tank_size_gallons'], 
        "Swing Tank Element" : site_df.loc[selected_table, 'swing_element_kw'], 
        "Temperature Maintenance Storage Volume" : site_df.loc[selected_table, 'swing_tank_volume'],
        "Schematic Drawing": f"![]({app.get_asset_url('schematic-swingtank-small.jpg')})" if not (swing_tank_volume is None or pd.isna(swing_tank_volume)) else None
    }

    detail = []
    info = []

    for key, value in mapping.items():
        if not (value is None or pd.isna(value)):
            detail.append(key)
            info.append(value)

    df = pd.DataFrame({
        "Detail": detail,
        "Information": info
    })

    return html.Div([
        html.H2("Building Metadata"),
        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in df.columns],
            style_cell={'textAlign': 'left'},
            style_as_list_view=True,
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            },
        ),
    ])

def get_no_raw_retrieve_msg():
    return html.P(style={'color': 'black', 'textAlign': 'center'}, children=[
            html.Br(),
            f"To view raw data, please select the 'Retrieve Raw Data' checkbox and re-run the query. It is recommended to only retrieve raw data for a few days at a time to avoid long loading times."
        ])