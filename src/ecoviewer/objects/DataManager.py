import mysql.connector
import pandas as pd
import math

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
    """
    def __init__(self, raw_data_creds : dict, config_creds : dict, user_email : str, selected_table : str = None, start_date : str = None, end_date : str = None, checkbox_selections : list = []):
        self.raw_data_creds = raw_data_creds
        self.config_creds = config_creds
        self.last_called_mysql = None
        self._check_mysql_creds()
        self.site_df, self.graph_df, self.field_df = self.get_user_permissions_from_db(user_email, self.config_creds)
        self.selected_table = selected_table
        if self.site_df.empty:
            raise Exception("User does not have permission to access data.")
        elif self.selected_table is None:
            self.selected_table = self.site_df.index.tolist()[0]

        self.min_table = self.site_df.loc[self.selected_table, 'minute_table']
        self.hour_table = self.site_df.loc[self.selected_table, 'hour_table']
        self.day_table = self.site_df.loc[self.selected_table, 'daily_table']
        self.db_name = self.site_df.loc[self.selected_table, 'db_name']
        self.state_tracking = self.site_df.loc[self.selected_table, 'state_tracking']

        self.start_date = start_date
        self.end_date = end_date
        self.raw_df = None
        self.organized_mapping = None
        self.checkbox_selections = checkbox_selections

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
    
    def _round_df_to_3_decimal(self, df : pd.DataFrame) -> pd.DataFrame:
        float_cols = df.select_dtypes(include=['float64'])
        df[float_cols.columns] = float_cols.round(3)
        return df
    
    def get_df_from_query(self, query : str) -> pd.DataFrame:
        cnx = mysql.connector.connect(
            host=self.raw_data_creds['host'],
            user=self.raw_data_creds['user'],
            password=self.raw_data_creds['password'],
            database=self.db_name
        )
        cursor = cnx.cursor()

        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        cursor.close()
        cnx.close()
        if not df.empty:
            df = df.set_index('time_pt')
            # round float columns to 3 decimal places
            df = self._round_df_to_3_decimal(df)

        return df
    
    def get_raw_data_df(self, all_fields : bool = False) -> pd.DataFrame: 
        if self.raw_df is None:
            # raw df has not already been generated
            query = self.generate_raw_data_query()
            self.raw_df = self.get_df_from_query(query)
        if self.organized_mapping is None:
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
    def get_organized_mapping(self, df_columns : list, all_fields : bool = False):
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
        site_fields = site_fields.set_index('field_name')
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
