import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Dash, dash_table
from ecoviewer.config import get_organized_mapping
from ecoviewer.constants.constants import *
from ecoviewer.objects.DataManager import DataManager

def create_meta_data_table(dm : DataManager, app : Dash, anonymize_data : bool = True) -> html.Div:
    """
    Parameters
    ----------
    dm : DataManager
        The DataManager object for the current data pull
    app: Dash
        The dash application that this table is being created for. This parameter must be passed to access it's assets for schematic images
    anonymize_data : bool
        set to True to remove sensitive identifying data (e.g. the building's address) from the data table. False to leave it in. Defaults to True 

    Returns
    -------
    meta_data_table: html.Div
        html div that contains a two column table to describe the site's meta data
    """
    wh_unit_name = dm.get_attribute_for_site('wh_unit_name')
    wh_manufacturer = dm.get_attribute_for_site('wh_manufacturer')
    primary_model = None
    if not wh_manufacturer is None and not wh_unit_name is None:
        primary_model = f"{wh_manufacturer} {wh_unit_name}"
    elif not wh_manufacturer is None:
         primary_model = f"{wh_manufacturer}"
    elif not wh_unit_name is None:
         primary_model = f"{wh_unit_name}"
    swing_tank_volume = dm.get_attribute_for_site('swing_tank_volume')
    zip_code = dm.get_attribute_for_site('zip_code')
    swing_t_elem = dm.get_attribute_for_site('swing_element_kw')
    primary_volume = dm.get_attribute_for_site('tank_size_gallons')
    installation_year = dm.get_attribute_for_site('unit_installation_year')
    notes = dm.get_attribute_for_site('notes')
    occupant_capacity = dm.get_attribute_for_site('occupant_capacity')
    building_specs = "Unknown"
    if dm.get_attribute_for_site('building_specs') is not None:
        building_specs = dm.get_attribute_for_site('building_specs')
    elif dm.get_attribute_for_site('building_type') is not None:
        building_specs = dm.get_attribute_for_site('building_type')

    schematic_img = dm.get_attribute_for_site('custom_dict_display_1')
    if schematic_img is None and not (swing_tank_volume is None or pd.isna(swing_tank_volume)):
        schematic_img = 'schematic-swingtank.jpg'

    additional_img = dm.get_attribute_for_site('custom_dict_display_2')

    mapping = {
        "Address" : dm.get_attribute_for_site('address') if dm.get_attribute_for_site('address') is not None else "Unknown", 
        "Zip Code" : zip_code if not (zip_code is None or pd.isna(zip_code)) else "Unknown",
        "NOAA Weather Station" : dm.get_attribute_for_site('noaa_station'),
        "Building Specifications/Type" : building_specs,
        "Number of Occupants" : f"{round(occupant_capacity)} Occupants" if not (occupant_capacity is None or pd.isna(occupant_capacity)) and occupant_capacity > 1 else None,
        "Primary System Model" : primary_model, 
        "Primary HPWHs" : dm.get_attribute_for_site('number_heat_pumps'), 
        "Primary Tank Volume" : f"{primary_volume} Gallons" if not (primary_volume is None or pd.isna(primary_volume)) else None, 
        "Swing Tank Element" : f"{swing_t_elem} kW" if not (swing_t_elem is None or pd.isna(swing_t_elem)) else None, 
        "Temperature Maintenance Storage Volume" : f"{swing_tank_volume} Gallons" if not (swing_tank_volume is None or pd.isna(swing_tank_volume)) else None,
        "Installation Year" : installation_year if not (installation_year is None or pd.isna(installation_year)) else None,
        "Operation Hours" : dm.get_attribute_for_site('operation_hours') if dm.get_attribute_for_site('operation_hours') is not None else None,
        "AWHS Climate" : dm.get_attribute_for_site('climate'),
        "Expected COP" : dm.get_attribute_for_site('expected_COP'),
        "Notes" : notes if not (notes is None or pd.isna(notes)) else None,
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
            style_data_conditional=[
                    {
                        'if': {'column_id': 'Detail'},
                        'backgroundColor': 'rgb(240, 240, 240)'
                    },
                ],
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            },
        ),
        get_display_schematic(dm,app)
    ])

def create_event_log_table(dm : DataManager, msg_p : html.P = None) -> html.Div:
    """
    Parameters
    ----------
    dm : DataManager
        The DataManager object for the current data pull
    msg_p : html.P
        html paragraph that carries an additional message to be attached to the end of the event log

    Returns
    -------
    event_log_table: html.Div
        html div that contains a table of all events from the queried site
    """
    try:
        event_df = dm.get_site_events(filter_by_date = False)
        if event_df.shape[0] == 0:
            # return error on empty event_df
            return html.Div([
                html.P(
                    style={'color': 'black', 'textAlign': 'center'}, 
                    children=[ html.Br(),"No event data available.", msg_p]
                )
            ])        
        event_df['start_time_pt'] = pd.to_datetime(event_df['start_time_pt']).dt.date
        if not 'end_time_pt' in event_df.columns:
            event_df.insert(2, 'end_time_pt', None)
        if event_df['end_time_pt'].notnull().any():
            event_df['end_time_pt'] = pd.to_datetime(event_df['end_time_pt']).dt.date
        if not dm.user_is_ecotope():
            event_df = event_df.drop(columns=['id'])
        event_df = event_df.rename(columns={
            'start_time_pt':'Start Date', 
            'end_time_pt' : 'End Date', 
            'event_type' : 'Event Type', 
            'event_detail' : 'Details'
        })
        
        return html.Div([
            html.H2("Event Log"),
            dash_table.DataTable(
                data=event_df.to_dict('records'),
                columns=[{"name": i, "id": i, "presentation": "markdown"} for i in event_df.columns],
                style_cell={'textAlign': 'left'},
                style_as_list_view=True,
                style_header={
                    'backgroundColor': 'rgb(230, 230, 230)',
                    'fontWeight': 'bold'
                },
                style_data_conditional=[
                    {
                        'if': {'column_id': 'Start Date'},
                        'backgroundColor': 'rgb(240, 240, 240)'
                    },
                    {
                        'if': {'column_id': 'Event Type'},
                        'backgroundColor': 'rgb(240, 240, 240)' 
                    },
                    {
                        'if': {'column_id': 'Details'}, 
                        'width': '70%',
                        'whiteSpace': 'normal',
                        'height': 'auto'
                    }
                ],
                sort_action='native',
                page_action='native',   # Enable pagination
                page_current=0,         # Start on the first page
                page_size=20           # Display 20 rows per page
            ),
            msg_p
        ])

    except Exception as e:
        return html.Div([
                html.P(
                    style={'color': 'red', 'textAlign': 'center'}, 
                    children=[html.Br(),f"Could not load event log: {str(e)}"]
                )
            ])

def get_display_schematic(dm : DataManager, app : Dash) -> html.Div:
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

    schematic_img = dm.get_attribute_for_site('custom_dict_display_1')
    additional_img = dm.get_attribute_for_site('custom_dict_display_2')

    if schematic_img is None and additional_img is None:
        return html.Div()
    
    images = [
        html.H2("System Schematic"),
    ]
    if not schematic_img is None:
        images.append(html.Img(
                style={'width':'100%'},
                src=app.get_asset_url(schematic_img)
            ))
    if not additional_img is None:
        images.append(html.Img(
                style={'width':'100%'},
                src=app.get_asset_url(additional_img)
            ))

    return html.Div(images)

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

def create_summary_table(dm : DataManager) -> html.Div:
    """
    Parameters
    ----------
    dm : DataManager
        The DataManager object for the current data pull
    """
    try:
        site_names = []
        zip_codes = []
        equipment_type = []
        expected_cop = []
        actual_cop = []
        ongoing_events = []
        event_descriptions = []
        all_sites = dm.site_df.index.tolist()
        for site in all_sites:
            exp_cop = dm.get_attribute_for_site("expected_COP", site_name=site)
            if not exp_cop is None and not pd.isna(exp_cop):
                zip_code = dm.get_attribute_for_site('zip_code', site_name=site)
                zip_codes.append(zip_code if not (zip_code is None or pd.isna(zip_code)) else "Unknown")
                site_dm = DataManager(dm.raw_data_creds,dm.config_creds,dm.user_email,site)
                site_names.append(site_dm.get_attribute_for_site("pretty_name"))
                wh_unit_name = site_dm.get_attribute_for_site('wh_unit_name')
                wh_manufacturer = site_dm.get_attribute_for_site('wh_manufacturer')
                primary_model = None
                if not wh_manufacturer is None and not wh_unit_name is None:
                    primary_model = f"{wh_manufacturer} {wh_unit_name}"
                elif not wh_manufacturer is None:
                    primary_model = f"{wh_manufacturer}"
                elif not wh_unit_name is None:
                    primary_model = f"{wh_unit_name}"
                equipment_type.append(primary_model)
                expected_cop.append(exp_cop)
                actual_cop.append(round(site_dm.get_average_cop(),2))
                site_ongoing_events = site_dm.get_ongoing_events()
                site_ongoing_event_descriptions = site_dm.get_ongoing_event_descriptions()
                ongoing_events.append(", ".join(site_ongoing_events) if len(site_ongoing_events) > 0 else "No ongoing events.")
                event_descriptions.append(" ".join(site_ongoing_event_descriptions) if len(site_ongoing_event_descriptions) > 0 else "N/A")

        df = pd.DataFrame({
            "Site": site_names,
            "Zip Code": zip_codes,
            "Equipment": equipment_type,
            "Expected COP": expected_cop,
            "Actual Average COP":actual_cop,
            "Ongoing Events": ongoing_events,
            "Details" : event_descriptions
        })

        return html.Div([
            html.H2("Summary"),
            dash_table.DataTable(
                data=df.to_dict('records'),
                columns=[{"name": i, "id": i, "presentation": "markdown"} for i in df.columns],
                style_cell={'textAlign': 'left'},
                style_as_list_view=True,
                style_data_conditional=[
                        {
                            'if': {'column_id': 'Zip Code'},
                            'backgroundColor': 'rgb(240, 240, 240)'
                        },
                        {
                            'if': {'column_id': 'Expected COP'},
                            'backgroundColor': 'rgb(240, 240, 240)'
                        },
                        {
                            'if': {'column_id': 'Ongoing Events'},
                            'backgroundColor': 'rgb(240, 240, 240)'
                        },
                        {
                            'if': {'column_id': 'Details'}, 
                            'width': '50%',
                            'whiteSpace': 'normal',
                            'height': 'auto'
                        }
                    ],
                style_header={
                    'backgroundColor': 'rgb(230, 230, 230)',
                    'fontWeight': 'bold'
                },
            )
        ])

    except Exception as e:
        return html.Div([
                html.P(
                    style={'color': 'red', 'textAlign': 'center'}, 
                    children=[html.Br(),f"Could not load summary table: {str(e)}"]
                )
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

def create_data_dictionary_checklist(dm : DataManager):
    organized_mapping = dm.get_organized_mapping([],True)
    
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
    """
    Parameters
    ----------
    user_email : str
        The email address of the user attempting to access data on dash app

    Returns
    -------
    no_permission_message : html.P
        an html element to tell the user they do not have appropriate permissions to view the data they are attempting to access
    """
    return html.P(style={'color': 'black', 'textAlign': 'center'}, children=[
            html.Br(),
            f"Permissions for {user_email} have not yet been set up. Please contact site administrator for assistance."
        ])