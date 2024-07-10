import pandas as pd
import mysql.connector
import math
from datetime import datetime, timedelta
from ecoviewer.constants.constants import *
import os
import configparser

def get_site_df_from_ini(config_file_path : str):
    """
    Parameters
    ----------
    config_file_path : str
        File Path to the config.ini file for the pipeline a user wishes to create the site_df from

    Returns
    -------
    site_df : pandas.DataFrame
        a data frame containing summary configuration and meta data for each site available in the dashapp for a user
    """
    os.chdir(os.getcwd())

    if not os.path.exists(config_file_path):
        raise Exception(f"File path '{config_file_path}' does not exist.")
    
    configure = configparser.ConfigParser()
    configure.read(config_file_path)

    if not 'minute' in configure:
        raise Exception(f"Missing minute table configuration in '{config_file_path}'.")
    
    data = {
        'site_name': [configure.get("minute", 'table_name')],
        'minute_table': [configure.get("minute", 'table_name')],
        'daily_table': [configure.get("day", 'table_name') if 'day' in configure else configure.get("minute", 'table_name')],
        'hour_table': [configure.get("hour", 'table_name') if 'hour' in configure else configure.get("minute", 'table_name')],
        'db_name': [configure.get('database', 'database')],
        'db_host': [configure.get('database', 'host')],
        'db_pw': [configure.get('database', 'password')],
        'db_user': [configure.get('database', 'user')]
    }

    for var_name in ["construction_year","zip_code","square_feet","sector","building_type","operation_hours",
                     "commercial_occupancy_type","occupant_capacity","cwh_location","wh_unit_name",
                        "wh_manufacturer","unit_installation_year","model_number","model_type","tank_size_gallons",
                        "address","building_specs","swing_tank_volume","swing_element_kw","number_heat_pumps","pretty_name"]:
        if configure.has_section('meta_data') and configure.has_option('meta_data', var_name):
            data[var_name] = [configure.get('meta_data', var_name)]
        elif var_name == "pretty_name":
            data[var_name] = [configure.get("minute", 'table_name')]
        else:
            data[var_name] = [None]

    for var_name in ["summary_bar_graph","summary_hour_graph","summary_pie_chart",
                          "summary_gpdpp_histogram",'summary_gpdpp_timeseries', 'summary_peaknorm', 'summary_hourly_flow',
                          'summary_cop_regression', 'summary_cop_timeseries','summary_flow_boxwhisker', "state_tracking",
                            "load_shift_tracking"]:
        if configure.has_section('summary_options') and configure.has_option('summary_options', var_name) and configure.get('summary_options', var_name).lower() == 'true':
            data[var_name] = [True]
        else:
            data[var_name] = [False]
        
    # Create DataFrame
    df = pd.DataFrame(data)
    table_names = df["site_name"].values.tolist()
    df = df.set_index('site_name')

    display_drop_down = []
    for name in table_names:
        display_drop_down.append({'label': df.loc[name, "pretty_name"], 'value' : name})

    return df, display_drop_down

def get_graph_df_from_csv(graph_csv_path : str):
    """
    Parameters
    ----------
    graph_csv_path : str
        File Path to the Graph_Config.csv file for the pipeline a user wishes to create the graph_df from

    Returns
    -------
    graph_df : pandas.DataFrame
        a data frame containing graph configuration data for each graph available in the dashapp for a user
    """
    if os.path.isfile(graph_csv_path):
        # Read the CSV file into a pandas DataFrame
        graph_df = pd.read_csv(graph_csv_path)
        required_columns = {'graph_id', 'graph_title', 'y_1_title', 'y_2_title'}
        if required_columns.issubset(graph_df.columns):
            graph_df['graph_title'] = graph_df.apply(lambda row: row['graph_id'] if pd.isnull(row['graph_title']) else row['graph_title'], axis=1)
            graph_df['y_1_title'] = graph_df.apply(lambda row: '' if pd.isnull(row['y_1_title']) else row['y_1_title'], axis=1)
            graph_df['y_2_title'] = graph_df.apply(lambda row: '' if pd.isnull(row['y_2_title']) else row['y_2_title'], axis=1)
            graph_df = graph_df.set_index('graph_id')
            return graph_df
        else:
            raise Exception(f"Column(s) missing from config file. could not process. Ensure all columns in {required_columns} are present in graph configuration csv")
    else:
        raise Exception(f"{graph_csv_path} does not exist.")
    
def get_field_df_from_csv(field_csv_path : str, site_name : str):
    """
    Parameters
    ----------
    field_csv_path : str
        File Path to the Variable_Names.csv file for the pipeline a user wishes to create the field_df from

    Returns
    -------
    field_df : pandas.DataFrame
        a data frame containing field configuration data for each field available in the dashapp for a user
    """
    if os.path.isfile(field_csv_path):
        # Read the CSV file into a pandas DataFrame
        field_df = pd.read_csv(field_csv_path)
        required_columns = {'data_type', "variable_name", "graph_id", "pretty_name", "descr", "secondary_axis"}
        if required_columns.issubset(field_df.columns):
            # pretty_names_df = field_df.copy()
            # pretty_names_df = pretty_names_df[(pretty_names_df['data_type'].isna()) & (pretty_names_df['pretty_name'].notnull())]

            # Filter rows with non-null data_type
            field_df = field_df[field_df['data_type'].notna()]
            # TODO also add filtering for system where neccessary
    
            # Iterate over the DataFrame and insert rows into the 'field' table
            field_df['field_name'] = field_df['variable_name']
            field_df['pretty_name'] = field_df.apply(lambda row: row['field_name'] if pd.isnull(row['pretty_name']) else row['pretty_name'], axis=1)
            field_df['description'] = field_df.apply(lambda row: row['pretty_name'] if pd.isnull(row['descr']) else row['descr'], axis=1)
            field_df['secondary_y'] = field_df.apply(lambda row: _getGraphInfo(row['data_type'], row['graph_id'], row['secondary_axis'])[1], axis=1)
            field_df['graph_id'] = field_df.apply(lambda row: _getGraphInfo(row['data_type'], row['graph_id'], row['secondary_axis'])[0], axis=1)
            field_df['site_name'] = site_name

            optional_fields = ['summary_group','upper_bound','lower_bound',"hourly_shapes_display"]
            for optional_field in optional_fields:
                    if optional_field in field_df.columns:
                        field_df[optional_field] = field_df.apply(lambda row: None if pd.isnull(row[optional_field]) else row[optional_field], axis=1)
           
            return field_df
    
        else:
            raise Exception(f"Column(s) missing from config file. could not process. Ensure all columns in {required_columns} are present in graph configuration csv")
    else:
        raise Exception(f"{field_csv_path} does not exist.")
    
# Function to get graph_id and secondary_y based on data_type
def _getGraphInfo(data_type, graph_input, secondary_axis):
    if graph_input != "default" and not pd.isna(graph_input):
        if not secondary_axis is None and isinstance(secondary_axis, bool):
            return(graph_input, secondary_axis)
        return(graph_input, False)
    if data_type == 'temp' or data_type == 'f':
        return ("tmp_default", False)
    elif data_type == 'heat_kw':
        return ("pwr_kw_default", True)
    elif data_type == 'kw':
        return ("pwr_kw_default", False)
    elif data_type == 'gpm':
        return ("flw_default", False)
    elif data_type == 'other':
        return ("cnds_default", True)
    elif data_type == 'gallons':
        return ("vlm_default", False)
    elif data_type == 'cop':
        return ("cop_default", False)
    elif data_type == 'cfm':
        return ("airflw_default", False)
    elif data_type == 'cop_instantaneous':
        return ("cop_efc_default", False)
    elif data_type == 'efficiency':
        return ("cop_efc_default", True)
    elif data_type == 'w':
        return("pwr_w_default", False)
    elif data_type == 'btuhr':
        return("pwr_w_default", True)
    else:
        return (None, None)

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
        )) ORDER BY pretty_name
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

    cursor.close()
    cnx.close()

    display_drop_down = []
    for name in table_names:
        display_drop_down.append({'label': site_df.loc[name, "pretty_name"], 'value' : name})
    return site_df, graph_df, field_df, display_drop_down

def get_organized_mapping(df_columns, graph_df : pd.DataFrame, field_df : pd.DataFrame, selected_table : str, all_fields : bool = False):
    """
    Parameters
    ----------
    df_columns: list
        list of all the column names present in the Pandas dataframe containing data from the site
    graph_df: pd.Dataframe
        Pandas dataframe containing all data for graphs from site configuration database. this should include 
        graph_id as the index,
        y_1_title as the title of y axis 1 for the graph
        y_2_title as the title of y axis 2 for the graph
        graph_title as the displayed title of the graph
    field_df: pd.Dataframe
        Pandas dataframe containing all data for fields from site configuration database. this should include 
        field_name - index of the df
        graph_id coresponding to the index of graph_df
        pretty_name as the displayed name of the field
        description as a description of the field for the data dictionary tab
        secondary_y - boolean column to determine which Y axis the field belongs to on the graph
        optional: lower_bound and upper_bound for filtering outliers out
    selected_table : str
        Name of the site that the mapping is being created for
    all_fields : bool
        set to True to get all fields including those not in the dataframe of site data 

    Returns
    -------
    organized_mapping: dictionary
        dictionary mapping each graph to a list of site data dataframe columns that belong to that graph in the form
        {
            graph_id : {
                "title" : graph_title,
                "y1_units" : y1_units,
                "y2_units" : y2_units,
                "y1_fields" : y1_fields,
                "y2_fields" : y2_fields
            }
        }
    """
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

def is_within_raw_data_limit(date_str1 : str, date_str2 : str):
    if date_str1 is None or date_str2 is None:
        return True
    date1 = datetime.strptime(date_str1, '%Y-%m-%d')
    date2 = datetime.strptime(date_str2, '%Y-%m-%d')
    difference = abs(date1 - date2)
    return difference <= timedelta(days=max_raw_data_days)

def round_df_to_3_decimal(df : pd.DataFrame) -> pd.DataFrame:
    float_cols = df.select_dtypes(include=['float64'])
    df[float_cols.columns] = float_cols.round(3)
    return df

def get_all_graph_ids(sql_dash_config):
    
    cnx = mysql.connector.connect(**sql_dash_config)
    cursor = cnx.cursor() 

    cursor.execute("SELECT DISTINCT graph_id FROM graph_display;")
    result = cursor.fetchall()

    column_names = [desc[0] for desc in cursor.description]
    graph_df = pd.DataFrame(result, columns=column_names)
    graph_ids = graph_df["graph_id"].values.tolist()

    cursor.close()
    cnx.close()
    
    return graph_ids