from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
from dash import dcc, html
import plotly.express as px

class GPDPPHistogram(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Daily Hot Water Usage Histogram", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_summary_data_df(self.summary_group)
        if pd.notna(dm.occupant_capacity) and self._is_numeric(dm.occupant_capacity) and dm.flow_variable in df_daily.columns:
            nTenants = dm.occupant_capacity # TODO get this from central site csv
            df_daily['DHWDemand'] = df_daily[dm.flow_variable]*60*24/nTenants
            fig = px.histogram(df_daily, x='DHWDemand', title='Domestic Hot Water Demand (' + str(int(nTenants)) + ' Tenants)',
                            labels={'DHWDemand': 'Gallons/Person/Day'})
            return dcc.Graph(figure=fig)
        else:
            if not (pd.notna(dm.occupant_capacity) and self._is_numeric(dm.occupant_capacity)):
                error_msg = "erroneous occupant_capacity in site configuration."
            else:
                error_msg = f"daily dataframe missing {dm.flow_variable}."
            return html.P(style={'color': 'red'}, children=[
                        f"Error: could not load GPDPP histogram due to {error_msg}"
                    ])
    
    def _is_numeric(self,value):
        return pd.api.types.is_numeric_dtype(pd.Series([value]))