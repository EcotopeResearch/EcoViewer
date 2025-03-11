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
    """
    An object that contains a graph for display 

    Attributes
    ----------
    dm : DataManager
        The DataManager object for the current data pull
    title : str
        The title of the Graph type. This will be displayed if there is an error to tell the user
        what graph could not be generated
    event_reports : list
        A list of event identifiers that need to be reported when they affect the graph
    event_filters : list
        A list of event identifiers that need to be filtered out of the data collected for the graph
    date_filtered : bool
        True if graph is filtered by the user input date range, False otherwise
    display_event_note : bool
        set to true if you would like a note reporting events to be displayed at bottom of graph. False if not
    """
    def __init__(self, dm : DataManager, title : str = "Graph", event_reports : list = [], event_filters : list = [], date_filtered : bool = True,
                 display_event_note : bool = False):
        self.title = title
        self.pkl_file_name = self.create_pkl_file_name(dm)
        self.event_reports = event_reports
        self.event_filters = event_filters
        self.date_filtered = date_filtered
        # load pickle if it exists
        if not self.pkl_file_name is None and self.check_if_file_exists(dm.pkl_folder_path):
            try:
                self._load_graph_from_pkl(dm.pkl_folder_path)
            except Exception as e:
                self.graph = self.get_error_msg(f"Could not load saved graph {self.title}: {str(e)}")
        else:
            try:
                self.graph = self.create_graph(dm)
                if display_event_note and len(self.event_reports) > 0:
                    if isinstance(self.graph, list):
                        self.graph.append(self.get_event_note(dm))
                    else:
                        self.graph = html.Div([
                            self.graph,
                            self.get_event_note(dm)
                        ])
            except Exception as e:
                self.graph = self.get_error_msg(f"Could not generate {self.title}: {str(e)}")

    def create_graph(self, dm : DataManager):
        # TODO add reset to default date message
        return None
    
    def get_graph(self):
        return self.graph
    
    def get_event_note(self, dm : DataManager):
        event_df = self.get_events_in_timeframe(dm)
        seen_filtered_events = False
        if event_df.shape[0] <= 0:
            # possibly add in a note for  what types of events are filtered?
            return None
        
        # For huge lists
        counts = event_df["event_type"].value_counts()
        large_instance_events = counts[counts > 3]
        filtered_event_df = event_df[~event_df["event_type"].isin(large_instance_events.index)]

        note_list = ["Above graph includes data collected during the following events:",html.Br()]
        for index, row in filtered_event_df.iterrows():
            start_date = row['start_time_pt'].strftime('%m-%d-%Y')
            if not 'end_time_pt' in filtered_event_df.columns or row['end_time_pt'] is None or pd.isna(row['end_time_pt']):
                end_date = 'ONGOING'
            else:
                end_date = row['end_time_pt'].strftime('%m-%d-%Y')
            if row['event_type'] in self.event_filters:
                note_list.append(f"{row['event_type']}*: {start_date} - {end_date}")
                seen_filtered_events = True
            else:
                note_list.append(f"{row['event_type']}: {start_date} - {end_date}")
            note_list.append(html.Br())
        for event_type, event_count in large_instance_events.items():
            if event_type in self.event_filters:
                note_list.append(f"{event_type}*: {event_count} instances")
                seen_filtered_events = True
            else:
                note_list.append(f"{event_type}: {event_count} instances")
            note_list.append(html.Br())
        note_list.append("Check Event Log tab for more information.")
        if seen_filtered_events:
            note_list.append(html.Br())
            note_list.append("*Event occured during timeframe but is filtered out of graph data")
        return html.P(style={'color': 'red', 'textAlign': 'left'}, children=note_list)
    
    def get_events_in_timeframe(self, dm : DataManager):
        return dm.get_site_events(filter_by_date = self.date_filtered, event_types=self.event_reports, 
                                      start_date=dm.start_date, end_date=dm.end_date)

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


