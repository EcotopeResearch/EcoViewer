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
        if self.summary_group is None:
            raise Exception("Summary Group not configured for site.")
        df = dm.get_daily_summary_data_df(self.summary_group,self.event_filters)
        if df.shape[0] <= 0:
            raise Exception("No data availabe for time period.")
        if dm.start_date is None and dm.end_date is None:
            self.start_day = df.index[0]
            self.end_day = df.index[-1]

        powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(powerin_columns, True)
        sorted_columns = sorted(zip(power_pretty_names, powerin_columns), key=lambda x: x[0])

        power_pretty_names = [x[0] for x in sorted_columns]
        sorted_columns = [x[1] for x in sorted_columns]
        sums = df[sorted_columns].sum()
        if (sums == 0).all():
            raise Exception("No energy used for time span selected.")
        power_colors = dm.get_color_list(sums.index.tolist())
        pie_fig = px.pie(names=power_pretty_names, values=sums.values.round(1), 
                         title=f"<b>Distribution of Energy Consumption<br><span style='font-size:14px;'>{get_date_range_string(df)}</span>",
                         color_discrete_sequence=power_colors,
                         category_orders={'names': power_pretty_names}
                        )
        pie_fig.update_layout(title_font=dict(size=24))
        pie_fig.update_traces(textfont_size=18)
        return dcc.Graph(figure=pie_fig)