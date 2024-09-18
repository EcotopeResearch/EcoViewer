from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc
import plotly.colors

class HourlyShapesPlots(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Hourly Shapes Plots"):
        super().__init__(dm, title)
        self.state_colors = {
            "loadUp" : "green",
            "shed" : "blue"
        }

    def clean_df(self, df : pd.DataFrame, organized_mapping):
        for key, value in organized_mapping.items():
            fields = value["y1_fields"] + value["y2_fields"]

            # Iterate over the values and add traces to the figure
            for field_dict in fields:
                column_name = field_dict["column_name"]
                if 'lower_bound' in field_dict:
                    df[column_name] = np.where(df[column_name] < field_dict["lower_bound"], np.nan, df[column_name])

                if 'upper_bound' in field_dict:
                    df[column_name] = np.where(df[column_name] > field_dict["upper_bound"], np.nan, df[column_name])

    def create_graph(self, dm : DataManager):
        if not dm.is_within_raw_data_limit():
            return dm.get_no_raw_retrieve_msg()
        graph_components = []
        graph_components = dm.add_default_date_message(graph_components)
        df, organized_mapping = dm.get_raw_data_df(all_fields=False, hourly_fields_only=True)
        if df.empty:
            raise Exception("No data available for parameters specified.")
        weekday_df = df[df['weekday'] == True]
        weekend_df = df[df['weekday'] == False]
        weekday_df = weekday_df.groupby('hr').mean(numeric_only = True)
        weekend_df = weekend_df.groupby('hr').mean(numeric_only = True)
        subplot_titles = []
        for key, value in organized_mapping.items():
            # Extract the category (e.g., Temperature or Power)
            category = value["title"]
            subplot_titles.append(f"<b>{category} weekday</b>")
            subplot_titles.append(f"<b>{category} weekend</b>")

        
        # Create a new figure for the category
        fig = make_subplots(rows = len(organized_mapping.items())*2, cols = 1, 
                    specs=[[{"secondary_y": True}]]*len(organized_mapping.items())*2,
                    shared_xaxes=True,
                    vertical_spacing = 0.1/max(1, len(organized_mapping.items())),
                    subplot_titles = subplot_titles)
        
        row = 1
        colors = plotly.colors.DEFAULT_PLOTLY_COLORS
        color_num = 0

        for key, value in organized_mapping.items():
            # Extract the category (e.g., Temperature or Power)
            category = value["title"]

            # Extract the y-axis units
            y1_units = value["y1_units"]
            y2_units = value["y2_units"]

            # Extract the values for the category
            y1_fields = value["y1_fields"]
            y2_fields = value["y2_fields"]

            # Iterate over the values and add traces to the figure
            y_axis = 'y1'
            secondary_y = False
            for field_dict in y1_fields:
                name = field_dict["readable_name"]
                column_name = field_dict["column_name"]
                if column_name in weekday_df.columns:
                    #line color
                    line_color = field_dict["color"]
                    if line_color is None:
                        line_color = colors[color_num]
                        color_num += 1
                        color_num = color_num % len(colors)

                    weekday_trace = go.Scatter(x=weekday_df.index, y=weekday_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                                hovertemplate="<br>".join([
                                                    f"{name}",
                                                    "hour=%{x}",
                                                    "value=%{y}",
                                                ]), 
                                                line=dict(color = line_color))
                    weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines', 
                                                hovertemplate="<br>".join([
                                                    f"{name}",
                                                    "hour=%{x}",
                                                    "value=%{y}",
                                                ]), 
                                                showlegend=False, line=dict(color = line_color))
                    fig.add_trace(weekday_trace, row=row, col = 1, secondary_y=secondary_y)
                    fig.add_trace(weekend_trace, row=row+1, col = 1, secondary_y=secondary_y)

            y_axis = 'y2'
            secondary_y = True
            for field_dict in y2_fields:
                name = field_dict["readable_name"]
                column_name = field_dict["column_name"]
                if column_name in weekday_df.columns:
                    #line color
                    line_color = field_dict["color"]
                    if line_color is None:
                        line_color = colors[color_num]
                        color_num += 1
                        color_num = color_num % len(colors)
                    weekday_trace = go.Scatter(x=weekday_df.index, y=weekday_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                                hovertemplate="<br>".join([
                                                    f"{name}",
                                                    "hour=%{x}",
                                                    "value=%{y}",
                                                ]),
                                                line=dict(color = line_color))
                    weekend_trace = go.Scatter(x=weekend_df.index, y=weekend_df[column_name], name=name, legendgroup=name, yaxis=y_axis, mode='lines',
                                                hovertemplate="<br>".join([
                                                    f"{name}",
                                                    "hour=%{x}",
                                                    "value=%{y}",
                                                ]), 
                                                showlegend=False, line=dict(color = line_color))
                    fig.add_trace(weekday_trace, row=row, col = 1, secondary_y=secondary_y)
                    fig.add_trace(weekend_trace, row=row+1, col = 1, secondary_y=secondary_y)

            fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row, col = 1, secondary_y = False)
            fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row, col = 1, secondary_y = True)
            fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row+1, col = 1, secondary_y = False)
            fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row+1, col = 1, secondary_y = True)

            row += 2

        fig.update_xaxes(title_text="<b>Hour</b>", row = row, col = 1)
        fig.update_layout(
            width=1300,
            height=len(organized_mapping.items())*460)

        figure = go.Figure(fig)
        # Add the figure to the array of graph objects
        graph_components.append(dcc.Graph(figure=figure))

        return graph_components