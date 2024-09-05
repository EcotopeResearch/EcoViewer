import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots
import plotly.colors
import numpy as np
import math
from ecoviewer.objects.DataManager import DataManager
from datetime import datetime


class GraphObject:
    def __init__(self, dm : DataManager, title : str = "Graph"):
        self.title = title
        try:
            self.graph = self.create_graph(dm)
        except Exception as e:
            self.graph = self.get_error_msg(e)

    def create_graph(self, dm : DataManager):
        # TODO add reset to default date message
        return None
    
    def get_graph(self):
        return self.graph
    
    def get_error_msg(self, e : Exception):
        return html.P(
            style={'color': 'red', 'textAlign': 'center'}, 
            children=[
                html.Br(),
                f"Could not generate {self.title}: {str(e)}"
            ]
        )

