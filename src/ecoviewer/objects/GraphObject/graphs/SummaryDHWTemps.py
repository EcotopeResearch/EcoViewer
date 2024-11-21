from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px
import plotly.graph_objects as go

class SummaryDHWTemps(GraphObject):
    def __init__(self, dm : DataManager, title : str = "DHW Temps", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df, organized_mapping = dm.get_raw_data_df(self.summary_group,['PIPELINE_ERR'])
        print(organized_mapping['color'])
        if df.shape[0] <= 0:
            raise Exception("No data availabe for time period.")
        
        temp_cols = ["Temp_DHWSupply", "Temp_MXVHotInlet", "Temp_StorageHotOutlet"]
        selected_columns = [col for col in df.columns if any(temp_col in col for temp_col in temp_cols) and "Temp_DHWSupply2" not in col]
        
        names = dm.get_pretty_names(selected_columns, False)[1]
        #print(test)
        #powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
        #sums = df[powerin_columns].sum()
        #power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(sums.index.tolist(), True)
        # sums = sums.sort_values(ascending=False)
        #power_colors = dm.get_color_list(sums.index.tolist())
        #box_fig = px.pie(names=power_pretty_names, values=sums.values, title='<b>Distribution of Energy',
        #                 color_discrete_sequence=power_colors,
        #                 category_orders={'names': power_pretty_names}
        #                )
        fig = go.Figure()
    
        for col in selected_columns:
            name = names[col]
            if col in df.columns:
                fig.add_trace(go.Box(y = df[col], name = '<b>' + name))

        fig.update_layout(title="<b>DHW Temperatures", yaxis_title=" ")




        return dcc.Graph(figure=fig)
    

