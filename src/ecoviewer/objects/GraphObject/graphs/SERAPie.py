from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px

class SERAPie(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Annual EUI", summary_group : str = None):
        self.summary_group = summary_group
        self.graph_type = "summary_SERA_pie"
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df, start_date, end_date = dm.get_annual_minute_df()
    
        # df = df.resample('T').asfreq()
        # df = df.bfill()
        
        power_data = df[['PowerIn_Lighting', 'PowerIn_PlugsMisc', 'PowerIn_Ventilation', 'PowerIn_HeatingCooling', 'PowerIn_DHW']].sum() / 60 * 3.41 / 39010

        name_mapping = {'PowerIn_Lighting':'Lighting', 'PowerIn_PlugsMisc':'Plugs/Misc', 'PowerIn_Ventilation':'Ventilation',
                        'PowerIn_HeatingCooling':'Heating/Cooling', 'PowerIn_DHW':'Domestic Hot Water'}

        mapped_names = power_data.index.map(name_mapping)
        
        colors = px.colors.qualitative.Antique

        if start_date == '08/01/2023':
            power_data['PowerIn_PlugsMisc'] += 2805 * 3.41 / 39010

        fig = px.pie(names = mapped_names, values = power_data.values.round(2), title = '<b>Annual EUI:</b><br>' + start_date + ' - ' + end_date,
                    color_discrete_sequence = [colors[4], colors[1], colors[2], colors[3], colors[5]])
        
        fig.update_traces(
            textinfo='percent+label',  
            texttemplate='%{percent:.1%}<br>%{value}', 
            hovertemplate='%{percent:.1%}<br>%{value}')
        
        return dcc.Graph(figure=fig)