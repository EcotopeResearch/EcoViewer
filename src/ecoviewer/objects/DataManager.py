import mysql.connector

class DataManager:
    def __init__(self, raw_data_creds : dict, config_creds : dict):
        self.raw_data_creds = raw_data_creds
        self.config_creds = config_creds
        self.last_called_mysql = None

    def _check_mysql_creds(self):
        required_keys = {'host', 'user', 'password', 'database'}
        if not required_keys.issubset(self.raw_data_creds.keys()):
            raise Exception("incomplete mySQL credentials for site data database")
        if not required_keys.issubset(self.config_creds.keys()):
            raise Exception("incomplete mySQL credentials for configuration data database")
