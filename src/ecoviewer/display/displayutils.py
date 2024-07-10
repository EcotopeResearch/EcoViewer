import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Dash, dash_table
from ecoviewer.config import get_organized_mapping
from ecoviewer.constants.constants import *

def create_meta_data_table(site_df : pd.DataFrame, selected_table : str, app : Dash, anonymize_data : bool = True) -> html.Div:
    """
    Parameters
    ----------
    site_df: pd.Dataframe
        Pandas dataframe representing containing all meta data for each data site the user has access to.
    selected_table : str
        Name of the site that the meta data table is being created for. This string should corespond to an index in site_df
    app: Dash
        The dash application that this table is being created for. This parameter must be passed to access it's assets for schematic images
    anonymize_data : bool
        set to True to remove sensitive identifying data (e.g. the building's address) from the data table. False to leave it in. Defaults to True 

    Returns
    -------
    meta_data_table: html.Div
        html div that contains a two column table to describe the site's meta data
    """
    wh_unit_name = site_df.loc[selected_table, 'wh_unit_name']
    wh_manufacturer = site_df.loc[selected_table, 'wh_manufacturer']
    primary_model = None
    if not wh_manufacturer is None and not wh_unit_name is None:
        primary_model = f"{wh_manufacturer} {wh_unit_name}"
    elif not wh_manufacturer is None:
         primary_model = f"{wh_manufacturer}"
    elif not wh_unit_name is None:
         primary_model = f"{wh_unit_name}"
    swing_tank_volume = site_df.loc[selected_table, 'swing_tank_volume']
    zip_code = site_df.loc[selected_table, 'zip_code']
    swing_t_elem = site_df.loc[selected_table, 'swing_element_kw']
    primary_volume = site_df.loc[selected_table, 'tank_size_gallons']
    installation_year = site_df.loc[selected_table, 'unit_installation_year']
    building_specs = "Unknown"
    if site_df.loc[selected_table, 'building_specs'] is not None:
        building_specs = site_df.loc[selected_table, 'building_specs']
    elif site_df.loc[selected_table, 'building_type'] is not None:
        building_specs = site_df.loc[selected_table, 'building_type']

    mapping = {
        "Address" : site_df.loc[selected_table, 'address'] if site_df.loc[selected_table, 'address'] is not None else "Unknown", 
        "Zip Code" : zip_code if not (zip_code is None or pd.isna(zip_code)) else "Unknown",
        "Building Specifications/Type" : building_specs, 
        "Primary System Model" : primary_model, 
        "Primary HPWHs" : site_df.loc[selected_table, 'number_heat_pumps'], 
        "Primary Tank Volume" : f"{primary_volume} Gallons" if not (primary_volume is None or pd.isna(primary_volume)) else None, 
        "Swing Tank Element" : f"{swing_t_elem} kW" if not (swing_t_elem is None or pd.isna(swing_t_elem)) else None, 
        "Temperature Maintenance Storage Volume" : f"{swing_tank_volume} Gallons" if not (swing_tank_volume is None or pd.isna(swing_tank_volume)) else None,
        "Installation Year" : installation_year if not (installation_year is None or pd.isna(installation_year)) else None,
        "Operation Hours" : site_df.loc[selected_table, 'operation_hours'] if site_df.loc[selected_table, 'operation_hours'] is not None else None,
        "Schematic Drawing": f"![]({app.get_asset_url('schematic-swingtank.jpg')})" if not (swing_tank_volume is None or pd.isna(swing_tank_volume)) else None,
    }

    if anonymize_data:
        mapping['Address'] = None


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

def get_no_raw_retrieve_msg() -> html.P:
    """
    Returns
    -------
    no_raw_retrieval_msg: html.P
        html component to communicate that time frame is too large to retrieve raw data
    """
    return html.P(style={'color': 'black', 'textAlign': 'center'}, children=[
            html.Br(),
            f"Time frame is too large to retrieve raw data. To view raw data, set time frame to {max_raw_data_days} days or less and ensure the 'Retrieve Raw Data' checkbox is selected."
        ])

def create_data_dictionary(organized_mapping):
    """
    Parameters
    ----------
    organized_mapping: pd.Dataframe
    
    Returns
    -------
    data_dictionary: list
        html div that contains a two column table to describe the site's meta data
    """
    returnStr = [html.H2('Variable Definitions', style={'font-size': '24px'})]
    for key, value in organized_mapping.items():
        returnStr.append(html.H3(value["title"], style={'font-size': '18px'}))
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

def create_data_dictionary_checklist(graph_df : pd.DataFrame, field_df : pd.DataFrame, selected_table : str):
    organized_mapping = get_organized_mapping([], graph_df, field_df, selected_table, True)
    returnDiv = []
    
    for key, value in organized_mapping.items():
        returnDiv.append(
            dcc.Checklist(
                id=f'checkbox-{value["title"]}',
                options=[
                    {'label': html.Div([value["title"]], style={'font-size': '18px', "font-weight": "bold"}), 
                     'value': key}
                ],
                value = [key],
                labelStyle={"display": "flex", "align-items": "center"}
            ),
        )

        y1_fields = value["y1_fields"]
        y2_fields = value["y2_fields"]
        sub_check_box_options = []
        sub_values = []
        for field_dict in y1_fields + y2_fields:
            descr = field_dict["description"]
            if descr is None:
                descr = ''
            sub_check_box_options.append({
                'label' : html.Div([
                    html.Span(''+field_dict["readable_name"]+'',style={"font-weight": "bold"}),
                    html.Span(' : '+descr,style={"font-weight": "normal"}),
                    ],
                    style={'font-size': '14px'}
                ),
                'value' : field_dict["column_name"]
            })
            sub_values.append(field_dict["column_name"])
        returnDiv.append(
            html.Div(
                [
                    dcc.Checklist(
                        id=f'checkbox-{value["title"]}-fields',
                        options=sub_check_box_options,
                        value = sub_values,
                        labelStyle={"display": "flex", "align-items": "center"}
                    ),
                    html.Br()
                ],
                style={'padding-left': '20px'}
            )
        )
    return returnDiv

def user_has_no_permisions_message(user_email : str):
    return html.P(style={'color': 'black', 'textAlign': 'center'}, children=[
            html.Br(),
            f"Permissions for {user_email} have not yet been set up. Please contact site administrator for assistance."
        ])