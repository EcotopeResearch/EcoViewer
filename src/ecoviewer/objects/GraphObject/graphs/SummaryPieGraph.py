from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from dash import dcc
import plotly.express as px

class SummaryPieGraph(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Distribution of Energy Pie Chart", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df = dm.get_daily_summary_data_df(self.summary_group)
        powerin_columns = [col for col in df.columns if col.startswith('PowerIn_') and 'PowerIn_Total' not in col and df[col].dtype == "float64"]
        sums = df[powerin_columns].sum()
        power_pretty_names, power_pretty_names_dict = dm.get_pretty_names(sums.index.tolist(), True)
        # sums = sums.sort_values(ascending=False)
        power_colors = dm.get_color_list(sums.index.tolist())
        pie_fig = px.pie(names=power_pretty_names, values=sums.values, title='<b>Distribution of Energy',
                         color_discrete_sequence=power_colors,
                         category_orders={'names': power_pretty_names}
                        )
        return dcc.Graph(figure=pie_fig)