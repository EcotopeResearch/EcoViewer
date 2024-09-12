import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots
import plotly.colors
import numpy as np
import pickle
from ecoviewer.objects.DataManager import DataManager
from datetime import datetime
import os


class GraphObject:
    def __init__(self, dm : DataManager, title : str = "Graph"):
        self.title = title
        self.pkl_file_name = self.create_pkl_file_name(dm)
        # load pickle if it exists
        if not self.pkl_file_name is None and self.check_if_file_exists(dm.pkl_folder_path):
            try:
                self._load_graph_from_pkl(dm.pkl_folder_path)
            except Exception as e:
                self.graph = self.get_error_msg(f"Could not load saved graph {self.title}: {str(e)}")
        else:
            try:
                self.graph = self.create_graph(dm)
            except Exception as e:
                self.graph = self.get_error_msg(f"Could not generate {self.title}: {str(e)}")

    def create_graph(self, dm : DataManager):
        # TODO add reset to default date message
        return None
    
    def get_graph(self):
        return self.graph
    
    def get_error_msg(self, error_str : str):
        return html.P(
            style={'color': 'red', 'textAlign': 'center'}, 
            children=[
                html.Br(),
                error_str
            ]
        )
    
    def create_pkl_file_name(self, dm : DataManager):
        if hasattr(self, 'graph_type'):
            return f"{dm.selected_table}_{self.graph_type}"
        return None
    
    def check_if_file_exists(self, folder_path : str, file_name : str = None):
        if not file_name is None:
            self.pkl_file_name = file_name
        if self.pkl_file_name is None or folder_path is None:
            return False
        file_path = os.path.join(folder_path, f"{self.pkl_file_name}.pkl")
        return os.path.isfile(file_path)
    
    def _load_graph_from_pkl(self, folder_path : str):
        with open(os.path.join(folder_path, f"{self.pkl_file_name}.pkl"), 'rb') as f:
            self.graph = pickle.load(f)
    
    def pickle_graph(self, folder_path : str, file_name : str = None):
        if not file_name is None:
            self.pkl_file_name = file_name
        if self.pkl_file_name is None:
            raise Exception("Cannot create pickled graph without a valid pickle file name.")
        if not os.path.exists(folder_path):
            raise Exception(f"Cannot create graph pickle. {folder_path} does not exist.")
        
        file_path_name = os.path.join(folder_path, f"{self.pkl_file_name}.pkl")
        with open(file_path_name, "wb") as f:
            pickle.dump(self.get_graph(), f)


