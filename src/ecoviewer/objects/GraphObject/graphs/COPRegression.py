from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px

class COPRegression(GraphObject):
    def __init__(self, dm : DataManager, title : str = "COP Regression", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=['EQUIPMENT_MALFUNCTION'])
        if not 'Temp_OutdoorAir' in df_daily.columns:
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]
        if not 'SystemCOP' in df_daily.columns:
            df_daily['SystemCOP'] = df_daily[dm.sys_cop_variable]

        fig = px.scatter(df_daily, x='Temp_OutdoorAir', y='SystemCOP',
                    title='<b>Outdoor Air Temperature & System COP Regression', trendline="ols", 
                    labels={'Temp_OutdoorAir': '<b>Outdoor Air Temperature', 'SystemCOP': '<b>System COP', 
                            'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
        color_discrete_sequence=["darkblue"]
                            )
        

        return dcc.Graph(figure=fig)