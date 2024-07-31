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
#import statsmodels.api as sm


class GraphObject:
    def __init__(self, title : str, y1_title : str, y2_title : str, dm : DataManager):
        self.title = title
        self.y1_title = y1_title
        self.y2_title = y2_title
        self.graph = self.create_graph(dm)

    def create_graph(self, dm : DataManager):
        return None
    
    def get_graph(self):
        return self.graph

