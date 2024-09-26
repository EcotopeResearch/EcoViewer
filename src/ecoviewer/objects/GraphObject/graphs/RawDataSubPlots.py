from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from dash import dcc

class RawDataSubPlots(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Raw Data Plots"):
        self.state_colors = {
            "loadUp" : "green",
            "shed" : "blue"
        }
        super().__init__(dm, title)

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
        df, organized_mapping = dm.get_raw_data_df(all_fields=False)
        if df.empty:
            raise Exception("No data available for parameters specified.")
        self.clean_df(df, organized_mapping)
        # Load the JSON data from the file
        subplot_titles = []
        for key, value in organized_mapping.items():
            # Extract the category (e.g., Temperature or Power)
            category = value["title"]
            subplot_titles.append(f"<b>{category}</b>")
        # Create a new figure for the category
        fig = make_subplots(rows = len(organized_mapping.items()), cols = 1, 
                    specs=[[{"secondary_y": True}]]*len(organized_mapping.items()),
                    shared_xaxes=True,
                    vertical_spacing = 0.1/max(1, len(organized_mapping.items())),
                    subplot_titles = subplot_titles)
        
        row = 0
        cop_columns = []

        for key, value in organized_mapping.items():
            row += 1
            # Extract the category (e.g., Temperature or Power)
            category = value["title"]

            # Extract the y-axis units
            y1_units = value["y1_units"]
            y2_units = value["y2_units"]

            # Extract the values for the category
            y1_fields = value["y1_fields"]
            y2_fields = value["y2_fields"]

            # Iterate over the values and add traces to the figure
            for field_dict in y1_fields:
                name = field_dict["readable_name"]
                column_name = field_dict["column_name"]
                y_axis = 'y1'
                secondary_y = False
                fig.add_trace(
                    go.Scatter(
                        x=df.index, 
                        y=df[column_name], 
                        name=name, 
                        yaxis=y_axis, 
                        mode='lines',
                        line=dict(color=field_dict["color"]),
                        hovertemplate="<br>".join([
                            f"{name}",
                            "time_pt=%{x}",
                            "value=%{y}",
                        ])
                    ), 
                    row=row, 
                    col = 1, 
                    secondary_y=secondary_y)
            for field_dict in y2_fields:
                name = field_dict["readable_name"]
                column_name = field_dict["column_name"]
                if 'COP' in column_name:
                    cop_columns.append(column_name)
                y_axis = 'y2'
                secondary_y = True
                fig.add_trace(
                    go.Scatter(
                        x=df.index, 
                        y=df[column_name], 
                        name=name, 
                        yaxis=y_axis, 
                        mode='lines',
                        line=dict(color=field_dict["color"]),
                        hovertemplate="<br>".join([
                            f"{name}",
                            "time_pt=%{x}",
                            "value=%{y}",
                        ])
                    ), 
                    row=row, 
                    col = 1, 
                    secondary_y=secondary_y)

            fig.update_yaxes(title_text="<b>"+y1_units+"</b>", row=row, col = 1, secondary_y = False)
            fig.update_yaxes(title_text="<b>"+y2_units+"</b>", row=row, col = 1, secondary_y = True)

        fig.update_xaxes(title_text="<b>Time</b>", row = row, col = 1)
        fig.update_layout(
            width=1500,
            height=len(organized_mapping.items())*350)

        # shading for system_state
        if dm.value_in_checkbox_selection('state_shading') and "system_state" in df.columns:
            # Create a boolean mask to identify the start of a new block
            df['system_state'].fillna('normal', inplace=True)
            state_change = df['system_state'] != df['system_state'].shift(1)

            # Use the boolean mask to find the start indices of each block
            state_change_indices = df.index[state_change].tolist()
            for i in range(len(state_change_indices)-1):
                change_time = state_change_indices[i]
                system_state = df.at[change_time, 'system_state']
                if system_state != 'normal':
                    fig.add_vrect(
                        x0=change_time, 
                        x1=state_change_indices[i+1],
                        fillcolor=self.state_colors[system_state], 
                        opacity=0.2,
                        layer="below", 
                        line_width=0,
                    )

            # Add the final vrect if needed
            if len(state_change_indices) > 0 and df.at[state_change_indices[-1], 'system_state'] != 'normal':
                system_state = df.at[state_change_indices[-1], 'system_state']
                fig.add_vrect(
                        x0=state_change_indices[-1],
                        x1=df.index[-1],
                        fillcolor=self.state_colors[system_state], 
                        opacity=0.2,
                        layer="below", 
                        line_width=0,
                    )

        figure = go.Figure(fig)

        # Add the figure to the array of graph objects
        graph_components.append(dcc.Graph(figure=figure))
        return graph_components
 