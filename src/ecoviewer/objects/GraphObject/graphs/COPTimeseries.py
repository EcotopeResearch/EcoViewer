from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.constants.constants import *
from dash import dcc
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from dash import html

class COPTimeseries(GraphObject):
    def __init__(self, dm : DataManager, title : str = "System COP Timeseries", summary_group : str = None):
        self.summary_group = summary_group
        self.seen_events = []
        self.cop_affecting_events = ['COMMISIONING','DATA_LOSS_COP','EQUIPMENT_MALFUNCTION','HW_OUTAGE','HW_LOSS',
            'INSTALLATION_ERROR','PARTIAL_OCCUPANCY','POWER_OUTAGE','SETPOINT_ADJUSTMENT','SYSTEM_MAINTENANCE']
        self.shader_colors = {
            'DATA_LOSS_COP' : 'red',
            'EQUIPMENT_MALFUNCTION' : 'orange',
            'HW_OUTAGE' : 'darkred',
            'HW_LOSS' : 'yellow',
            'PARTIAL_OCCUPANCY' : 'violet',
            'POWER_OUTAGE' : "grey",
            'SETPOINT_ADJUSTMENT' : 'darkviolet',
            'SYSTEM_MAINTENANCE' : 'green',
            'INSTALLATION_ERROR': 'darkblue',
            'COMMISIONING' : 'limegreen'
        }
        super().__init__(dm, title, event_reports=self.cop_affecting_events, date_filtered=False, event_filters=[],
                         display_event_note=True)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=self.event_filters)
        if not 'Temp_OutdoorAir' in df_daily.columns:
            if not dm.oat_variable in df_daily.columns:
                raise Exception('No outdoor air temperature data available.')
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]

        fig = make_subplots(specs = [[{'secondary_y':True}]])

        # deal with erroneous COP values 
        site_events_df = dm.get_site_events(event_types = self.cop_affecting_events)
        if site_events_df.shape[0] > 0:
            for index, row in site_events_df.loc[site_events_df['event_type'] == 'DATA_LOSS_COP'].iterrows():
                if row['end_time_pt'] is None or pd.isnull(row['end_time_pt']):
                    df_daily.loc[(df_daily.index >= row['start_time_pt']), dm.sys_cop_variable] = None
                else:
                    df_daily.loc[((df_daily.index >= row['start_time_pt']) & (df_daily.index <= row['end_time_pt'])), dm.sys_cop_variable] = None
                    

        fig.add_trace(go.Scatter(x = df_daily.index, y = df_daily[dm.sys_cop_variable],
                                mode = 'markers', name = dm.get_pretty_name(dm.sys_cop_variable),
                                marker=dict(color='darkred'), text=df_daily[dm.sys_cop_variable].apply(lambda val: f"{val:.1f}")), 
                                secondary_y = True)
        
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

        # state shading for events
        if site_events_df.shape[0] > 0:
            for index, row in site_events_df.iterrows():
                    
                if row['start_time_pt'] >= df_daily.index[-1]:
                    continue
                elif row['start_time_pt'] < df_daily.index[0]:
                    row['start_time_pt'] = df_daily.index[0]
                
                if not 'end_time_pt' in site_events_df.columns or row['end_time_pt'] is None or pd.isnull(row['end_time_pt']):
                    row['end_time_pt'] = df_daily.index[-1]
                elif row['end_time_pt'] <= df_daily.index[0]:
                    continue
                elif row['end_time_pt'] > df_daily.index[-1]:
                    row['end_time_pt'] = df_daily.index[-1]
                
                if not row['event_type'] in self.seen_events:
                    self.seen_events.append(row['event_type'])

                fig.add_vrect(
                    x0=row['start_time_pt'], 
                    x1=row['end_time_pt'],
                    fillcolor=self.shader_colors[row['event_type']], #self.state_colors[system_state]
                    opacity=0.2,
                    layer="below", 
                    line_width=0,
                )

        return dcc.Graph(figure=fig)
    
    def get_event_note(self, dm : DataManager):
        if len(self.seen_events) <= 0:
            return None
        color_key = html.Div(
            className='color-key',
            children=[
                html.Div(
                    className='legend-title',
                    children='Shading Legend for COP Timeseries Graph:'
                ),
                *[ 
                    html.Div(
                        children=[
                            html.Span(children=f"{event_type}: ",), 
                            html.Span(
                                style={
                                    'background-color': self.shader_colors[event_type],
                                    'display': 'inline-block',
                                    'width': '15px',
                                    'height': '15px',
                                    'opacity': '0.2',
                                    'margin-right': '5px'
                                }
                            )
                        ]
                    )
                    for event_type in self.seen_events
                ]
            ],
        )
        return color_key
