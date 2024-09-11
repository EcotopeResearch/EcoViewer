from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px
import plotly.graph_objects as go

class SERAMonthly(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Monthly Energy Consumption", summary_group : str = None):
        self.summary_group = summary_group
        self.graph_type = "summary_SERA_monthly"
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df, start_date, end_date = dm.get_annual_minute_df()
    
        # df['month'] = df.index.month
        # df = df.resample('T').asfreq()
        # df = df.bfill()

        power_data = df[['PowerIn_Lighting', 'PowerIn_PlugsMisc', 'PowerIn_Ventilation', 'PowerIn_HeatingCooling', 'PowerIn_DHW','Panel_2E57_Power_kW']]
        power_data = power_data.copy()

        monthly_data = power_data.resample('M').sum() / 60 
        
        if start_date == '08/01/2023':
            monthlyAvg = monthly_data.loc[monthly_data.index != '2023-08-31', 'Panel_2E57_Power_kW'].mean()        
            monthly_data.loc[monthly_data.index == '2023-08-31', 'PowerIn_PlugsMisc'] += monthlyAvg

        monthly_data.drop(columns = {'Panel_2E57_Power_kW'}, inplace = True)
        power_data.drop(columns = {'Panel_2E57_Power_kW'}, inplace = True)
        
        name_mapping = {'PowerIn_Lighting':'Lighting', 'PowerIn_PlugsMisc':'Plugs/Misc', 'PowerIn_Ventilation':'Ventilation',
                        'PowerIn_HeatingCooling':'Heating/Cooling', 'PowerIn_DHW':'Domestic Hot Water'}
        colors = px.colors.qualitative.Antique

        EUI = monthly_data.sum(axis=1).sum() * 3.41 / 39010

        fig = go.Figure()
        
        for i, col in enumerate(power_data.columns):
            fig.add_trace(go.Bar(
            x=monthly_data.index.strftime('%b'), 
            y=monthly_data[col].round(2), 
            name=name_mapping[col],  
            marker=dict(color=colors[i + 1]),
            hovertemplate='%{y:.0f} kWh<extra></extra>' 
        ))
            
        fig.update_layout(
            barmode='stack',  
            title='<b>Monthly Energy Consumption</b><br>' + str(int(round(EUI, 0))) + ' kBTU/sf/yr',
            xaxis_title='<b>Month',
            yaxis_title='<b>Energy Consumption (kWh)',
            legend_title='Category',
            title_x=0.5,  
            template='plotly_white' 
        )


        return dcc.Graph(figure=fig)