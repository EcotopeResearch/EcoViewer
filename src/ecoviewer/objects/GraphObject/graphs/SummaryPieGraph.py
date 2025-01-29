from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.display.displayutils import get_date_range_string
from dash import dcc
from ecoviewer.constants.constants import *
import plotly.express as px

class SummaryPieGraph(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Distribution of Energy Pie Chart", summary_group : str = None):
        self.summary_group = summary_group
        self.start_day = dm.start_date
        self.end_day = dm.end_date
        super().__init__(dm, title,event_reports=typical_tracked_events, event_filters=['DATA_LOSS_COP'])

    def get_events_in_timeframe(self, dm : DataManager):
        return dm.get_site_events(filter_by_date = self.date_filtered, event_types=self.event_reports, 
                                      start_date=self.start_day, end_date=self.end_day)
    def create_graph(self, dm : DataManager):
        df = dm.get_daily_summary_data_df(self.summary_group,self.event_filters)
        if df.shape[0] <= 0:
            raise Exception("No data availabe for time period.")
        if dm.start_date is None and dm.end_date is None:
            self.start_day = df.index[0]
            self.end_day = df.index[-1]
        powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
        sums = df[powerin_columns].sum()
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(sums.index.tolist(), True)
        # sums = sums.sort_values(ascending=False)
        power_colors = dm.get_color_list(sums.index.tolist())
        pie_fig = px.pie(names=power_pretty_names, values=sums.values, 
                         title=f"<b>Distribution of Energy<br><span style='font-size:14px;'>{get_date_range_string(df)}</span>",
                         color_discrete_sequence=power_colors,
                         category_orders={'names': power_pretty_names}
                        )
        return dcc.Graph(figure=pie_fig)