import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Dash, dash_table
from ecoviewer.config import get_organized_mapping

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
        "Schematic Drawing": f"![]({app.get_asset_url('schematic-swingtank.jpg')})" if not (swing_tank_volume is None or pd.isna(swing_tank_volume)) else None
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

def create_data_dictionary(organized_mapping):
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
    returnStr = []
    
    for key, value in organized_mapping.items():
        returnStr.append(
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
        returnStr.append(
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
    return returnStr

def user_has_no_permisions_message(user_email : str):
    return html.P(style={'color': 'black', 'textAlign': 'center'}, children=[
            html.Br(),
            f"Permissions for {user_email} have not yet been set up. Please contact site administrator for assistance."
        ])