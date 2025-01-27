from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.display.displayutils import get_date_range_string
from dash import dcc
import plotly.express as px
import plotly.graph_objects as go

class SummaryDHWTemps(GraphObject):
    def __init__(self, dm : DataManager, title : str = "DHW Temperatures", summary_group : str = None):
        self.summary_group = summary_group
        super().__init__(dm, title)

    def create_graph(self, dm : DataManager):
        df = dm.apply_event_filters_to_df(dm.get_raw_data_df()[0], ['PIPELINE_ERR'])
        
        # special filter
        if dm.get_attribute_for_site("flow_filter") > 0:
            df = df[df[dm.flow_variable] >= dm.get_attribute_for_site("flow_filter")].copy()

        if df.shape[0] <= 0:
            raise Exception("No data availabe for time period.")
        
        # default tracked temperatures 
        temp_cols = ["Temp_DHWSupply", "Temp_MXVHotInlet", "Temp_StorageHotOutlet", "Temp_HotOutlet"]
        # additional tracked temperatures custom per site
        for i in range(1,3):
            tracked_temp = dm.get_attribute_for_site(f"tracked_temperature_{i}")
            if not tracked_temp is None:
                temp_cols.append(tracked_temp) 

        selected_columns = [col for col in df.columns if any(temp_col in col for temp_col in temp_cols) and "Temp_DHWSupply2" not in col]
        
        names = dm.get_pretty_names(selected_columns, False)[1]
        colors = dm.get_color_list(selected_columns)

        fig = go.Figure()

        x_pos = 0  # Initialize x-position for primary axis box plots
        x_pos_secondary = len(selected_columns) - 1  # Start x-position for secondary y-axis box plots
        x_labels = [""] * len(selected_columns)

        for col, color in zip(selected_columns, colors):
            if 'CityWater' in col:
                # put on a different axis (because of likely temp difference) to make graph clearer
                fig.add_trace(go.Box(
                    y = df[col], name = '<b>' + names[col],
                    marker = dict(color = color), yaxis='y2',
                ))
                fig.update_layout(
                    yaxis2=dict(
                        title="City Water Temperature (F)",
                        overlaying="y",
                        side="right"
                    ),
                    legend=dict(
                        x=1.05,
                    )
                )
                x_labels[x_pos_secondary] = '<b>' + names[col]
                x_pos_secondary -= 1
            else:
                fig.add_trace(go.Box(
                    y = df[col], name = '<b>' + names[col],
                    marker = dict(color = color)
                ))
                x_labels[x_pos] = '<b>' + names[col]
                x_pos += 1

        fig.update_layout(title=f"<b>DHW Temperatures<br><span style='font-size:14px;'>{get_date_range_string(df)}</span>", 
                          yaxis_title="DHW Temperature (F)",
                          xaxis=dict(
                            categoryorder="array",  # Use custom ordering
                            categoryarray=x_labels,  # Specify the order
                        )
        )

        return dcc.Graph(figure=fig)
    

