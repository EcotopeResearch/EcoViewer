from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px
import plotly.graph_objects as go

class SummaryDHWTemps(GraphObject):
    def __init__(self, dm : DataManager, title : str = "DHW Temperatures", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df = dm.get_raw_data_df(self.summary_group,['PIPELINE_ERR'])[0]
        # special filter
        if dm.get_attribute_for_site("flow_filter") > 0:
            df = df[df[dm.flow_variable] >= dm.get_attribute_for_site("flow_filter")].copy()

        if df.shape[0] <= 0:
            raise Exception("No data availabe for time period.")
        # default tracked temperatures 
        temp_cols = ["Temp_DHWSupply", "Temp_MXVHotInlet", "Temp_StorageHotOutlet", "Temp_HotOutlet"]
        # additional tracked temperatures custom per site
        for i in range(1,3):
            tracked_temp = dm.get_attribute_for_site(f"tracked_temperature_{i}")
            if not tracked_temp is None:
                temp_cols.append(tracked_temp) 

        selected_columns = [col for col in df.columns if any(temp_col in col for temp_col in temp_cols) and "Temp_DHWSupply2" not in col]
        
        names = dm.get_pretty_names(selected_columns, False)[1]
        colors = dm.get_color_list(selected_columns)

        fig = go.Figure()
    
        for col, color in zip(selected_columns, colors):
            fig.add_trace(go.Box(y = df[col], name = '<b>' + names[col], marker = dict(color = color)))

        fig.update_layout(title="<b>DHW Temperatures", yaxis_title=" ")

        return dcc.Graph(figure=fig)
    

