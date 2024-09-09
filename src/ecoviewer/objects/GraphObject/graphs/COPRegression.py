from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px

class COPRegression(GraphObject):
    def __init__(self, dm : DataManager, title : str = "COP Regression", summary_group : str = None, cop_column : str = None):
        self.summary_group = summary_group
        self.cop_column = cop_column
        self.custom_cop_column = True
        if cop_column is None:
            self.custom_cop_column = False
            self.cop_column = dm.sys_cop_variable
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=['EQUIPMENT_MALFUNCTION','PIPELINE_ERR'])
        if not 'Temp_OutdoorAir' in df_daily.columns:
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]
        df_daily = df_daily[df_daily[self.cop_column] > 0]
        title='<b>Outdoor Air Temperature & System COP Regression'
        if self.custom_cop_column:
            title=f'<b>Outdoor Air Temperature & System COP Regression ({self.cop_column})'
        fig = px.scatter(df_daily, x='Temp_OutdoorAir', y=self.cop_column,
                    title=title, trendline="ols", 
                    labels={'Temp_OutdoorAir': '<b>Outdoor Air Temperature', f'{self.cop_column}': '<b>COP', 
                            'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
        color_discrete_sequence=["darkblue"]
                            )
        

        return dcc.Graph(figure=fig)