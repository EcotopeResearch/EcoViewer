from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.constants.constants import *
from dash import dcc
from plotly.subplots import make_subplots
import plotly.graph_objects as go

class COPTimeseries(GraphObject):
    def __init__(self, dm : DataManager, title : str = "System COP Timeseries", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title, event_reports=typical_tracked_events, date_filtered=False, event_filters=['DATA_LOSS_COP'])

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=self.event_filters)
        if not 'Temp_OutdoorAir' in df_daily.columns:
            if not dm.oat_variable in df_daily.columns:
                raise Exception('No outdoor air temperature data available.')
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]

        fig = make_subplots(specs = [[{'secondary_y':True}]])
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.sys_cop_variable],
                                mode = 'markers', name = dm.get_pretty_name(dm.sys_cop_variable),
                                marker=dict(color='darkred')), secondary_y = True)
        
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.oat_variable],
                                mode = 'markers', name = 'Outdoor Air Temerature',
                                marker=dict(color='darkgreen')), secondary_y = False)
        
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.city_water_temp],
                                mode = 'markers', name = 'City Water Temperature',
                                marker=dict(color='darkblue')), secondary_y = False)

        fig.update_layout(title = f'<b>{self.title}')
        fig.update_xaxes(title = '<b>Date')
        fig.update_yaxes(title = '<b>System COP', secondary_y = True)
        fig.update_yaxes(title = '<b>Daily Average Air and Water Temperature (F)', secondary_y = False)

        return dcc.Graph(figure=fig)