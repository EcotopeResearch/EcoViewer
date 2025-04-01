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
                                mode = 'markers', name = '<b>' + dm.get_pretty_name(dm.sys_cop_variable),
                                marker=dict(color='firebrick')), secondary_y = True)
        
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.oat_variable].round(1),
                                mode = 'markers', name = '<b>Outdoor Air Temp (F)',
                                marker=dict(color='olivedrab')), secondary_y = False)
        
        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.city_water_temp].round(1),
                                mode = 'markers', name = '<b>City Water Temp (F)',
                                marker=dict(color='rgb(56,166,165)')), secondary_y = False)

      
        fig.update_layout(
        title={'text': f'<b>{self.title}</b>', 'font': {'size': 24}},
        xaxis={'title': '<b>Date', 'title_font': {'size': 18}, 'tickfont': {'size': 18}},
        yaxis={'title': '<b>Daily Average Temperature (F)</b>', 'title_font': {'size': 18}, 'tickfont': {'size': 18}},
        yaxis2={'title': '<b>System COP</b>', 'title_font': {'size': 18}, 'tickfont': {'size': 18}})

        return dcc.Graph(figure=fig)