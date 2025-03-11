from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.display.displayutils import get_date_range_string
from ecoviewer.constants.constants import *
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
        #WIP
        # self.intercept = 0.0
        # self.slope = 0.0
        if cop_column is None:
            self.custom_cop_column = False
            self.cop_column = dm.sys_cop_variable
        super().__init__(dm, title, event_reports=typical_tracked_events, event_filters=['EQUIPMENT_MALFUNCTION','DATA_LOSS_COP'])

    def create_graph(self, dm : DataManager):
        if dm.system_is_swing_tank() and not 'PARTIAL_OCCUPANCY' in dm.get_ongoing_events():
            self.event_filters.append('PARTIAL_OCCUPANCY')
        if not 'INSTALLATION_ERROR' in dm.get_ongoing_events():
            self.event_filters.append('INSTALLATION_ERROR')
        df_daily = dm.get_daily_data_df(events_to_filter=self.event_filters)
        if not 'Temp_OutdoorAir' in df_daily.columns:
            if not dm.oat_variable in df_daily.columns:
                raise Exception('No outdoor air temperature data available.')
            df_daily['Temp_OutdoorAir'] = df_daily[dm.oat_variable]
        df_daily = df_daily[df_daily[self.cop_column] > 0]
        df_daily = df_daily[df_daily['Temp_OutdoorAir'].notna()]
        df_daily['Date'] = pd.to_datetime(df_daily.index).date
        # create graph
        title=f"<b>Outdoor Air Temperature & System COP Regression<br><span style='font-size:14px;'>{get_date_range_string(df_daily)}</span>"
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

            #WIP
            # self.intercept = wls_result.params['const']
            # self.slope = wls_result.params['Temp_OutdoorAir']

            # Get the predicted values for the trendline
            df_daily['trendline'] = wls_result.predict(X_with_const)
            fig = px.scatter(df_daily, x='Temp_OutdoorAir', y=self.cop_column,
                        title=title,
                        size=self.power_col,
                        labels={'Temp_OutdoorAir': '<b>Dry Bulb OAT (°F)', 
                                f'{self.cop_column}': f"<b>{dm.get_pretty_name(self.cop_column)}", 
                                'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
                        color_discrete_sequence=["darkblue"],
                        hover_data={'Date': True, self.cop_column: ':.1f'}
                )
            fig.add_traces(
                px.line(df_daily, x='Temp_OutdoorAir', y='trendline').data
            )
        else:
            fig = px.scatter(df_daily, x='Temp_OutdoorAir', y=self.cop_column,
                        title=title, trendline="ols",
                        labels={'Temp_OutdoorAir': '<b>Dry Bulb OAT (°F)', 
                        f'{self.cop_column}': f"<b>{dm.get_pretty_name(self.cop_column)}", 
                                'PrimaryEneryRatio': 'Primary Energy Ratio', 'Site': '<b>Site'},
                        color_discrete_sequence=["darkblue"],
                        hover_data={'Date': True, self.cop_column: ':.1f'}
                )
            # WIP
            # trendline_results = px.get_trendline_results(fig)
            # ols_model = trendline_results.iloc[0]["px_fit_results"].params  # Get statsmodels OLS results
            # self.slope =ols_model[0]
            # self.intercept=ols_model[1]
        
        # print(f"Weighted Trendline equation: y = {self.slope:.2f}x + {self.intercept:.2f}")
        return dcc.Graph(figure=fig)