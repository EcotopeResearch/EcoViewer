from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px
import pandas as pd
import statsmodels.api as sm

class COPRegression(GraphObject):
    def __init__(self, dm : DataManager, title : str = "COP Regression", summary_group : str = None, cop_column : str = None, power_col : str = None):
        self.summary_group = summary_group
        self.cop_column = cop_column
        self.custom_cop_column = True
        self.power_col = power_col
        if cop_column is None:
            self.custom_cop_column = False
            self.cop_column = dm.sys_cop_variable
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df_daily = dm.get_daily_data_df(events_to_filter=['EQUIPMENT_MALFUNCTION','PIPELINE_ERR'])
        if not 'Temp_OutdoorAir' in df_daily.columns:
            if not dm.oat_variable in df_daily.columns:
                raise Exception('No outdoor air temperature data available.')
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]
        df_daily = df_daily[df_daily[self.cop_column] > 0]
        df_daily = df_daily[df_daily['Temp_OutdoorAir'].notna()]
        df_daily['Date'] = pd.to_datetime(df_daily.index).date
        # create graph
        title='<b>Outdoor Air Temperature & System COP Regression'
        if self.custom_cop_column:
            title=f'<b>Outdoor Air Temperature (OAT) & Space Cooling COP Regression'
        if not self.power_col is None and self.power_col in df_daily.columns.tolist():
             # Prepare data for weighted OLS
            X = df_daily['Temp_OutdoorAir']
            y = df_daily[self.cop_column]
            weights = df_daily[self.power_col]
            # Add constant term to the predictor (required by statsmodels)
            X_with_const = sm.add_constant(X)
            # Perform Weighted Least Squares regression
            wls_model = sm.WLS(y, X_with_const, weights=weights)
            wls_result = wls_model.fit()
            # Get the predicted values for the trendline
            df_daily['trendline'] = wls_result.predict(X_with_const)
            fig = px.scatter(df_daily, x='Temp_OutdoorAir', y=self.cop_column,
                        title=title,
                        size=self.power_col,
                        labels={'Temp_OutdoorAir': '<b>Dry Bulb OAT (°F)', f'{self.cop_column}': '<b>COP', 
                                'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
                        color_discrete_sequence=["darkblue"],
                        hover_data={'Date': True}
                )
            fig.add_traces(
                px.line(df_daily, x='Temp_OutdoorAir', y='trendline').data
            )
        else:
            fig = px.scatter(df_daily, x='Temp_OutdoorAir', y=self.cop_column,
                        title=title, trendline="ols",
                        labels={'Temp_OutdoorAir': '<b>Dry Bulb OAT (°F)', f'{self.cop_column}': '<b>COP', 
                                'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
                        color_discrete_sequence=["darkblue"],
                        hover_data={'Date': True}
                )
        

        return dcc.Graph(figure=fig)