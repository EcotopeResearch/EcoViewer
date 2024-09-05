from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px

class DHWBoxWhisker(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Hourly DHW Usage Boxwhisker", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        hourly_df = dm.get_hourly_flow_data_df()
        hourly_df['hour'] = hourly_df.index.hour
        hourly_df['Flow_CityWater_PerTenant'] = hourly_df[dm.flow_variable] * 60 / dm.occupant_capacity

        fig = px.box(hourly_df, x = 'hour', y = 'Flow_CityWater_PerTenant', color_discrete_sequence=['darkblue'])
        fig.update_layout(title = '<b>Hourly DHW Usage')
        fig.update_xaxes(title = '<b>Hour')
        fig.update_yaxes(title = '<b>Gallons/Tenant')



        return dcc.Graph(figure=fig)