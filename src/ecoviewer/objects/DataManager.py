import mysql.connector
import pandas as pd
import math
from dash import html
from datetime import datetime, timedelta
from ecoviewer.constants.constants import *
from plotly.colors import DEFAULT_PLOTLY_COLORS

class DataManager:
    """
    Attributes
    ----------
    raw_data_creds : dict
        a dictionary containing the sql access information for the site data database. this should be in the form
        {
            'host':"host_name",
            'user':"mysql_user_name",
            'password':"mysql_pw"
        }
        database will be taken from the site details in the site database that can be accessed through the config_creds parameter
    config_creds : dict
        a dictionary containing the sql access information for the site configuration database. this should be in the form
        {
            'host':"host_name",
            'user':"mysql_user_name",
            'password':"mysql_pw",
            'database':"Site_Config_database_name"
        }
    user_email : str
        The email address of the current user - used for checking permissions
    selected_table : str
        The id of the selected data site
    start_date : str
        String representation for the start date of the timeframe
    end_date : str
        String representation for the start date of the timeframe
    checkbox_selections : list
        List of selected checkbox values from user request
    pkl_folder_path : str
        full path to directory conaining saved .pkl files for graph objects that use them
    """
    def __init__(self, raw_data_creds : dict, config_creds : dict, user_email : str, selected_table : str = None, start_date : str = None, end_date : str = None, checkbox_selections : list = [],
                 pkl_folder_path : str = None):
        self.pkl_folder_path = pkl_folder_path
        self.raw_data_creds = raw_data_creds
        self.config_creds = config_creds
        self.last_called_mysql = None
        self.user_email = user_email
        self._check_mysql_creds()
        self.site_df, self.graph_df, self.field_df = self.get_user_permissions_from_db(user_email, self.config_creds)
        self.selected_table = selected_table
        if self.site_df.empty:
            raise Exception("User does not have permission to access data.")
        elif self.selected_table is None:
            self.selected_table = self.site_df.index.tolist()[0]

        self.checkbox_selections = checkbox_selections
        self.min_table = self.site_df.loc[self.selected_table, 'minute_table']
        self.hour_table = self.site_df.loc[self.selected_table, 'hour_table']
        self.day_table = self.site_df.loc[self.selected_table, 'daily_table']
        self.db_name = self.site_df.loc[self.selected_table, 'db_name']
        self.state_tracking = self.site_df.loc[self.selected_table, 'state_tracking']
        self.load_shift_tracking = self.site_df.loc[self.selected_table, 'load_shift_tracking']
        self.occupant_capacity = self.site_df.loc[self.selected_table, 'occupant_capacity']

        self.start_date = start_date
        self.end_date = end_date
        self.raw_df = None
        self.daily_summary_df = None
        self.hourly_summary_df = None
        self.entire_daily_df = None
        self.entire_hourly_df = None
        self.organized_mapping = None
        self.annual_minute_df = None

        self.flow_variable = "Flow_CityWater"
        self.oat_variable= "Temp_OAT"
        self.sys_cop_variable = "COP_BoundaryMethod"
        self.sys_cop_variable_2 = None
        self.sys_cop_variable_3 = None
        self.sys_cop_variable_4 = None
        self.sys_power_variable = None
        self.sys_power_variable_2 = None
        self.sys_power_variable_3 = None
        self.sys_power_variable_4 = None
        if not self.site_df.loc[self.selected_table, 'flow_variable_name'] is None:
            self.flow_variable = self.site_df.loc[self.selected_table, 'flow_variable_name']
        if not self.site_df.loc[self.selected_table, 'oat_variable_name'] is None:
            self.oat_variable = self.site_df.loc[self.selected_table, 'oat_variable_name']
        if not self.site_df.loc[self.selected_table, 'sys_cop_variable_name'] is None:
            self.sys_cop_variable = self.site_df.loc[self.selected_table, 'sys_cop_variable_name']
        if not self.site_df.loc[self.selected_table, 'sys_cop_variable_name_2'] is None:
            self.sys_cop_variable_2 = self.site_df.loc[self.selected_table, 'sys_cop_variable_name_2']
        if not self.site_df.loc[self.selected_table, 'sys_cop_variable_name_3'] is None:
            self.sys_cop_variable_3 = self.site_df.loc[self.selected_table, 'sys_cop_variable_name_3']
        if not self.site_df.loc[self.selected_table, 'sys_cop_variable_name_4'] is None:
            self.sys_cop_variable_4 = self.site_df.loc[self.selected_table, 'sys_cop_variable_name_4']
        if not self.site_df.loc[self.selected_table, 'sys_power_variable'] is None:
            self.sys_power_variable = self.site_df.loc[self.selected_table, 'sys_power_variable']
        if not self.site_df.loc[self.selected_table, 'sys_power_variable_2'] is None:
            self.sys_power_variable_2 = self.site_df.loc[self.selected_table, 'sys_power_variable_2']
        if not self.site_df.loc[self.selected_table, 'sys_power_variable_3'] is None:
            self.sys_power_variable_3 = self.site_df.loc[self.selected_table, 'sys_power_variable_3']
        if not self.site_df.loc[self.selected_table, 'sys_power_variable_4'] is None:
            self.sys_power_variable_4 = self.site_df.loc[self.selected_table, 'sys_power_variable_4']
        self.display_reset_to_default_date_msg = None

    def needs_reset_to_default_date_msg(self):
        if self.display_reset_to_default_date_msg is None:
            if self.start_date is None or self.end_date is None:
                self.display_reset_to_default_date_msg = False
            else:
                query = f"SELECT time_pt FROM {self.min_table} WHERE {self.min_table}.time_pt >= '{self.start_date}' AND {self.min_table}.time_pt <= '{self.end_date} 23:59:59' LIMIT 1"
                if len(self.get_fetch_from_query(query)) > 0:
                    self.display_reset_to_default_date_msg = False
                else:
                    self.display_reset_to_default_date_msg = True
                    self.start_date = None
                    self.end_date = None
        return self.display_reset_to_default_date_msg
    
    def create_date_note(self):
        """
        returns [date_note, first_date, last_date]
        """
        query = f"SELECT time_pt FROM {self.min_table} ORDER BY time_pt ASC LIMIT 1"
        result = self.get_fetch_from_query(query)
        if len(result) == 0 or len(result[0]) == 0:
            return "If no date range is filled, The last three days of raw data and last 30 days of summary data will be returned."
        first_date = result[0][0]

        query = f"SELECT time_pt FROM {self.min_table} ORDER BY time_pt DESC LIMIT 1"
        result = self.get_fetch_from_query(query)
        last_date = result[0][0]

        return [
                f"Possible range for {self.site_df.loc[self.selected_table, 'pretty_name']}:",
                html.Br(),
                f"{first_date.strftime('%m/%d/%y')} - {last_date.strftime('%m/%d/%y')}",
                html.Br(),
                "If no date range is filled, The last three days of raw data and last 30 days of summary data will be returned."
        ]
    
    def add_default_date_message(self, graph_components: list) -> list:
        if self.needs_reset_to_default_date_msg():
            graph_components.append(html.P(style={'color': 'red', 'textAlign': 'center'}, children=[
                html.Br(),
                "No data available for date range selected. Defaulting to most recent data."
            ]))
        return graph_components
    
    def filter_graph_and_field_df(self, checklist_values):
        chosen_vals = self.parse_checklists_from_div(checklist_values)
        filtered_columns = [item for item in self.field_df['field_name'].tolist() if item in chosen_vals]
        filtered_graph_list = [item for item in self.graph_df.index if item in chosen_vals]

        # Filter the DataFrame to only those rows
        self.graph_df = self.graph_df.loc[filtered_graph_list]
        self.field_df = self.field_df[self.field_df['field_name'].isin(filtered_columns)]
        self.organized_mapping = None

    def get_selected_table(self):
        return self.selected_table

    def parse_checklists_from_div(self, div_children : list) -> list:
        ret_list = []
        for element in div_children:
            if 'type' in element:
                if element['type'] == 'Checklist':
                    ret_list = ret_list + element['props']['value']
                elif element['type'] == 'Div':
                    ret_list = ret_list + self.parse_checklists_from_div(element['props']['children'])
        return ret_list
    
    def is_within_raw_data_limit(self):
        if not self.value_in_checkbox_selection('get_raw_data'):
            return False
        if self.start_date is None or self.end_date is None:
            return True
        if self.user_email.split('@')[-1] == "ecotope.com": # ecotopers have no data limit
            return True
        date1 = datetime.strptime(self.start_date, '%Y-%m-%d')
        date2 = datetime.strptime(self.end_date, '%Y-%m-%d')
        difference = abs(date1 - date2)
        return difference <= timedelta(days=max_raw_data_days)

    def get_no_raw_retrieve_msg(self) -> html.P:
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

    def value_in_checkbox_selection(self, value : str):
        return value in self.checkbox_selections

    def _check_mysql_creds(self):
        if not {'host', 'user', 'password'}.issubset(self.raw_data_creds.keys()):
            raise Exception("Incomplete mySQL credentials for site data database")
        if not {'host', 'user', 'password', 'database'}.issubset(self.config_creds.keys()):
            raise Exception("Incomplete mySQL credentials for configuration data database")
    
    def get_user_permissions_from_db(self, user_email : str, sql_dash_config : dict, exclude_csv_only_fields : bool = True):
        """
        retrieves site_df, graph_df, field_df and table_names based on the permisions a user email has

        Parameters
        ----------
        user_email : str
            The email address of the user accessing the dash application
        sql_dash_config : dict
            a dictionary containing the sql access information for the site configuration database. this should be in the form
            {
                'host':"host_name",
                'user':"mysql_user_name",
                'password':"mysql_pw",
                'database':"Site_Config_database_name"
            }
        exclude_csv_only_fields : bool
            boolean to indicate whether to exclude fields  from field_df that should only be present when users download raw data csvs

        Returns
        -------
        site_df : pandas.DataFrame
            a data frame containing site configuration data for each datasite available in the dashapp for a user
        graph_df : pandas.DataFrame
            a data frame containing configuration data for each graph used in the dashapp
        field_df : pandas.DataFrame
            a data frame containing field configuration data for each field available in the dashapp for a user
        table_names : list
            a list of dictionaries that contain the appropriate displayed name and value for the dash applications site dropdown, taylored for the permissions of the user
        """
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

        return site_df, graph_df, field_df
    
    def get_table_dropdown(self):
        display_drop_down = []
        for name in self.site_df.index.to_list():
            display_drop_down.append({'label': self.site_df.loc[name, "pretty_name"], 'value' : name})
        return display_drop_down
    
    def get_attribute_for_site(self, attribute : str):
        if attribute in self.site_df.columns:
            return self.site_df.loc[self.selected_table, attribute]
        return None
    
    def graph_available(self, graph_type : str) -> bool:
        if graph_type in self.site_df.columns:
            return self.site_df.loc[self.selected_table, graph_type]
        return False

    def get_summary_groups(self):
        filtered_df = self.field_df[self.field_df['site_name'] == self.selected_table]
        filtered_df = filtered_df[filtered_df['summary_group'].notna()]
        return filtered_df['summary_group'].unique()
    
    def round_df_to_x_decimal(self, df : pd.DataFrame, x : int) -> pd.DataFrame:
        float_cols = df.select_dtypes(include=['float64'])
        df[float_cols.columns] = float_cols.round(x)
        return df
    
    def get_df_from_query(self, query : str, set_time_index : bool = True) -> pd.DataFrame:
        cnx = mysql.connector.connect(
            host=self.raw_data_creds['host'],
            user=self.raw_data_creds['user'],
            password=self.raw_data_creds['password'],
            database=self.db_name
        )
        cursor = cnx.cursor()
        # self.data_
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        cursor.close()
        cnx.close()
        df = df.dropna(axis=1, how='all')
        if set_time_index and not df.empty:
            df = df.set_index('time_pt')
            # round float columns to 3 decimal places
            df = self.round_df_to_x_decimal(df, 3)

        return df
    
    def get_fetch_from_query(self, query : str) -> list:
        cnx = mysql.connector.connect(
            host=self.raw_data_creds['host'],
            user=self.raw_data_creds['user'],
            password=self.raw_data_creds['password'],
            database=self.db_name
        )
        cursor = cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cnx.close()
        return result
    
    def generate_daily_summary_query(self, default_days = 30):
        summary_query = f"SELECT * FROM {self.day_table} "
        if self.start_date != None and self.end_date != None:
            summary_query += f"WHERE time_pt >= '{self.start_date}' AND time_pt <= '{self.end_date} 23:59:59' ORDER BY time_pt ASC"
        else:
            summary_query += f"ORDER BY time_pt DESC LIMIT {default_days}" #get last x days
            summary_query = f"SELECT * FROM ({summary_query}) AS subquery ORDER BY subquery.time_pt ASC;"
        return summary_query
    
    def get_daily_summary_data_df(self, summary_group : str = None, events_to_filter : list = []) -> pd.DataFrame:
        if self.daily_summary_df is None:
            # raw df has not already been generated
            query = self.generate_daily_summary_query()
            self.daily_summary_df = self.get_df_from_query(query)
            # filter for only fields that are assigned to be in summary tables
            if self.selected_table == 'bayview':
                # additional data prune for bayview
                self.daily_summary_df = self._bayview_prune_additional_power(self.daily_summary_df)
                self.daily_summary_df = self._bayview_power_processing(self.daily_summary_df)

            filtered_field_df = self.field_df[self.field_df['site_name'] == self.selected_table]
            filtered_df = filtered_field_df[filtered_field_df['summary_group'].notna()]
            group_columns = [col for col in self.daily_summary_df.columns if col in filtered_df['field_name'].tolist()]
            self.daily_summary_df = self.daily_summary_df[group_columns]

        if not summary_group is None:
            # filter for particular summary group
            filtered_group_df = self.field_df[self.field_df['site_name'] == self.selected_table]
            filtered_group_df = self.field_df[self.field_df['summary_group']==summary_group]
            group_columns = [col for col in self.daily_summary_df.columns if col in filtered_group_df['field_name'].tolist()]
            return self.daily_summary_df[group_columns]
        return self.apply_event_filters_to_df(self.daily_summary_df, events_to_filter)
    
    def get_pretty_names(self, column_names : list, replace_power_for_energy = False):
        pretty_names = []
        pretty_names_dict = {}
        filtered_field_df = self.field_df[self.field_df['site_name'] == self.selected_table]
        for column_name in column_names:
            pretty_name = filtered_field_df.loc[filtered_field_df['field_name'] == column_name, 'pretty_name'].values[0]
            if pretty_name is None:
                pretty_name = column_name
            if replace_power_for_energy:
                pretty_name = pretty_name.replace("power", "Energy")
                pretty_name = pretty_name.replace("Power", "Energy")
            pretty_names.append(pretty_name)
            pretty_names_dict[column_name] = pretty_name
        return pretty_names, pretty_names_dict
    
    def generate_hourly_summary_query(self, numHours = 740):
        if self.load_shift_tracking:
            hourly_summary_query = f"SELECT {self.hour_table}.*, HOUR({self.hour_table}.time_pt) AS hr, {self.day_table}.load_shift_day FROM {self.hour_table} " +\
                f"LEFT JOIN {self.day_table} ON {self.day_table}.time_pt = {self.hour_table}.time_pt "
        else:
            hourly_summary_query = f"SELECT {self.hour_table}.*, HOUR({self.hour_table}.time_pt) AS hr FROM {self.hour_table} "
        if self.start_date != None and self.end_date != None:
            hourly_summary_query += f"WHERE {self.hour_table}.time_pt >= '{self.start_date}' AND {self.hour_table}.time_pt <= '{self.end_date} 23:59:59' ORDER BY time_pt ASC"
        else:
            hourly_summary_query += f"ORDER BY {self.hour_table}.time_pt DESC LIMIT {numHours}" #get last 30 days plus some 740
            hourly_summary_query = f"SELECT * FROM ({hourly_summary_query}) AS subquery ORDER BY subquery.time_pt ASC;"
        return hourly_summary_query
    
    def get_hourly_summary_data_df(self, summary_group : str = None, events_to_filter : list = []) -> pd.DataFrame:
        if self.hourly_summary_df is None:
            if self.daily_summary_df is None:
                self.get_daily_summary_data_df(summary_group)
            # raw df has not already been generated
            query = self.generate_hourly_summary_query()
            self.hourly_summary_df = self.get_df_from_query(query)
            # filter for indexes between daily summary df bounds
            start_date = self.daily_summary_df.index.min()
            end_date = self.daily_summary_df.index.max() + pd.DateOffset(days=1)
            self.hourly_summary_df = self.hourly_summary_df[(self.hourly_summary_df.index >= start_date) & (self.hourly_summary_df.index <= end_date)]
            # ffill loadshifting or designate as all normal
            if 'load_shift_day' in self.hourly_summary_df.columns:
                self.hourly_summary_df["load_shift_day"] = self.hourly_summary_df["load_shift_day"].fillna(method='ffill') #ffill loadshift day
            else:
                self.hourly_summary_df["load_shift_day"] = 'normal'

            if self.selected_table == 'bayview':
                # additional data prune for bayview
                self.hourly_summary_df = self._bayview_prune_additional_power(self.hourly_summary_df)
                self.hourly_summary_df = self._bayview_power_processing(self.hourly_summary_df)

        return self.apply_event_filters_to_df(self.hourly_summary_df, events_to_filter)
    
    def _bayview_power_processing(self, df : pd.DataFrame) -> pd.DataFrame:
        df['PowerIn_SwingTank'] = df['PowerIn_ERTank1'] + df['PowerIn_ERTank2'] + df['PowerIn_ERTank5'] + df['PowerIn_ERTank6']

        # Drop the 'PowerIn_ER#' columns
        df = df.drop(['PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6'], axis=1)
        return df

    def _bayview_prune_additional_power(self, df : pd.DataFrame) -> pd.DataFrame:
        columns_to_keep = ['PowerIn_Swing', 'PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6', 'PowerIn_HPWH']
        columns_to_drop = [col for col in df.columns if col.startswith("PowerIn_") and col not in columns_to_keep]
        df = df.drop(columns=columns_to_drop)
        return df
    
    def get_raw_data_df(self, all_fields : bool = False, hourly_fields_only : bool = False): 
        if self.raw_df is None:
            # raw df has not already been generated
            query = self.generate_raw_data_query()
            self.raw_df = self.get_df_from_query(query)
            cop_columns = [col for col in self.raw_df.columns if 'COP' in col]
            self.raw_df[cop_columns] = self.raw_df[cop_columns].fillna(method='ffill')
            if 'OAT_NOAA' in self.raw_df.columns:
                self.raw_df["OAT_NOAA"] = self.raw_df["OAT_NOAA"].fillna(method='ffill')
            if 'system_state' in self.raw_df.columns:
                self.raw_df["system_state"] = self.raw_df["system_state"].fillna(method='ffill')
        if hourly_fields_only:
            return self.raw_df, self.get_organized_mapping(self.raw_df.columns, all_fields, hourly_fields_only)
        elif self.organized_mapping is None:
            self.organized_mapping = self.get_organized_mapping(self.raw_df.columns, all_fields)
        return self.raw_df, self.organized_mapping
        
    def generate_raw_data_query(self):
        query = f"SELECT {self.min_table}.*, "
        if self.state_tracking:
            query += f"{self.hour_table}.system_state, "
        
        # conditionals because some sites don't have these
        if self.field_df[(self.field_df['field_name'] == 'OAT_NOAA') & (self.field_df['site_name'] == self.selected_table)].shape[0] > 0:
            query += f"{self.hour_table}.OAT_NOAA, "
        if self.field_df[(self.field_df['field_name'] == 'COP_Equipment') & (self.field_df['site_name'] == self.selected_table)].shape[0] > 0:
            query += f"{self.day_table}.COP_Equipment, "
        if self.field_df[(self.field_df['field_name'] == 'COP_DHWSys_2') & (self.field_df['site_name'] == self.selected_table)].shape[0] > 0:
            query += f"{self.day_table}.COP_DHWSys_2, "
        query += f"IF(DAYOFWEEK({self.min_table}.time_pt) IN (1, 7), FALSE, TRUE) AS weekday, " +\
            f"HOUR({self.min_table}.time_pt) AS hr FROM {self.min_table} "
        #TODO these two if statements are a work around for LBNLC. MAybe figure out better solution
        if self.min_table != self.hour_table:
            query += f"LEFT JOIN {self.hour_table} ON {self.min_table}.time_pt = {self.hour_table}.time_pt "
        if self.min_table != self.day_table:
            query += f"LEFT JOIN {self.day_table} ON {self.min_table}.time_pt = {self.day_table}.time_pt "

        if self.start_date != None and self.end_date != None:
            query += f"WHERE {self.min_table}.time_pt >= '{self.start_date}' AND {self.min_table}.time_pt <= '{self.end_date} 23:59:59' ORDER BY {self.min_table}.time_pt ASC"
        else:
            query += f"ORDER BY {self.min_table}.time_pt DESC LIMIT 4000"
            query = f"SELECT * FROM ({query}) AS subquery ORDER BY subquery.time_pt ASC;"

        return query
    
    def get_daily_data_df(self, events_to_filter : list = []) -> pd.DataFrame:
        if self.entire_daily_df is None:
            query = f"SELECT * FROM {self.day_table};"
            self.entire_daily_df = self.get_df_from_query(query)
        return self.apply_event_filters_to_df(self.entire_daily_df, events_to_filter)


    def get_hourly_flow_data_df(self, events_to_filter : list = []) -> pd.DataFrame:
        if self.entire_hourly_df is None:
            query = f"SELECT time_pt, {self.flow_variable} FROM {self.hour_table};"
            self.entire_hourly_df = self.get_df_from_query(query)

        return self.apply_event_filters_to_df(self.entire_hourly_df, events_to_filter)
    
    def get_annual_minute_df(self, events_to_filter : list = []):
        if self.annual_minute_df is None:
            query = f"SELECT * FROM {self.min_table} ORDER BY time_pt DESC LIMIT 525600;" # 525600 minutes... How do you measure, measure a year?
            self.annual_minute_df = self.get_df_from_query(query)
            self.sera_last_day = self.annual_minute_df.index.max()
            self.sera_first_day = self.sera_last_day - pd.DateOffset(years=1) + pd.DateOffset(days=1)
            self.annual_minute_df = self.annual_minute_df.loc[(self.annual_minute_df.index >= self.sera_first_day) & (self.annual_minute_df.index <= self.sera_last_day)]
            self.annual_minute_df['month'] = self.annual_minute_df.index.month
            self.annual_minute_df = self.annual_minute_df.resample('T').asfreq()
            self.annual_minute_df = self.annual_minute_df.bfill()
        # check that df contains a year of data
        # first_day_boundary = last_day - pd.DateOffset(years=1) + pd.DateOffset(days=14)
        # if first_day_boundary < self.annual_minute_df.index.min():
        #     raise Exception("Not enough data. A years worth of data is required to produce this graph.")
        return self.apply_event_filters_to_df(self.annual_minute_df, events_to_filter), self.sera_first_day.strftime('%m/%d/%Y'), self.sera_last_day.strftime('%m/%d/%Y')
    
    def apply_event_filters_to_df(self, df : pd.DataFrame, events_to_filter : list):
        # TODO this could be optimized with a hash map of already searched filters
        if len(events_to_filter) > 0:
            filtered_df = df.copy()
            query = f"SELECT start_time_pt, end_time_pt FROM site_events WHERE site_name = '{self.selected_table}' AND event_type IN ("
            query = f"{query}'{events_to_filter[0]}'"
            for event_type in events_to_filter[1:]:
                query = f"{query},'{event_type}'"
            query = f"{query});"

            time_ranges = self.get_fetch_from_query(query)
            time_ranges = [(pd.to_datetime(start_time), pd.to_datetime(end_time)) for start_time, end_time in time_ranges]

            # Remove points in the DataFrame whose indexes fall within the time ranges
            for start_time, end_time in time_ranges:
                filtered_df = filtered_df.loc[~((filtered_df.index >= start_time) & (filtered_df.index <= end_time))]
            return filtered_df
        return df
    
    def get_color_list(self, df_columns: list, i : int = 0) -> list:
        color_list = []
        filtered_field_df = self.field_df[self.field_df['site_name'] == self.selected_table]
        # for i in range(len(df_columns)):
        #     df_column = df_columns[i]
        for df_column in df_columns:
            color = None
            if filtered_field_df[filtered_field_df['field_name'] == df_column].shape[0] == 1:
                color = filtered_field_df.loc[filtered_field_df['field_name'] == df_column, 'color'].values[0]
            if color is None:
                color = DEFAULT_PLOTLY_COLORS[i % len(DEFAULT_PLOTLY_COLORS)]
                i = i+1
            color_list.append(color)
        return color_list

    
    def get_organized_mapping(self, df_columns : list, all_fields : bool = False, hourly_fields_only : bool = False):
        """
        Parameters
        ----------
        df_columns: list
            list of all the column names present in the Pandas dataframe containing data from the site
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
        site_fields = self.field_df[self.field_df['site_name'] == self.selected_table]
        if hourly_fields_only:
            site_fields = site_fields[site_fields['hourly_shapes_display'] == True]
        site_fields = site_fields.set_index('field_name')
        site_fields = site_fields.sort_values(by='pretty_name', ascending=True)
        for index, row in self.graph_df.iterrows():
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
                    column_details['color'] = field_row["color"]
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
