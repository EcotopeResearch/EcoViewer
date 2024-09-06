import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
import numpy as np
import time
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.objects.GraphObject.graphs.SummaryPieGraph import SummaryPieGraph
from ecoviewer.objects.GraphObject.graphs.SummaryBarGraph import SummaryBarGraph
from ecoviewer.objects.GraphObject.graphs.SummaryDailyPowerByHour import SummaryDailyPowerByHour
from ecoviewer.objects.GraphObject.graphs.GPDPPTimeseries import GPDPPTimeseries
from ecoviewer.objects.GraphObject.graphs.GPDPPHistogram import GPDPPHistogram
from ecoviewer.objects.GraphObject.graphs.PeakNorm import PeakNorm
from ecoviewer.objects.GraphObject.graphs.SummaryHourlyFlow import SummaryHourlyFlow
from ecoviewer.objects.GraphObject.graphs.COPRegression import COPRegression
from ecoviewer.objects.GraphObject.graphs.COPTimeseries import COPTimeseries
from ecoviewer.objects.GraphObject.graphs.DHWBoxWhisker import DHWBoxWhisker
from ecoviewer.objects.GraphObject.graphs.ERVPerformance import ERVPerformance
from ecoviewer.objects.GraphObject.graphs.OHPPerformance import OHPPerformance
from ecoviewer.objects.GraphObject.graphs.SERAPie import SERAPie
from ecoviewer.objects.GraphObject.graphs.SERAMonthly import SERAMonthly
from ecoviewer.objects.GraphObject.graphs.RawDataSubPlots import RawDataSubPlots
from ecoviewer.objects.GraphObject.graphs.HourlyShapesPlots import HourlyShapesPlots


state_colors = {
    "loadUp" : "green",
    "shed" : "blue"}

def get_state_colors():
    return state_colors

def update_graph_time_frame(value, start_date, end_date, df, unit):
    dff = pd.DataFrame()
    if not isinstance(value, list):
        value = [value]
    if start_date != None and end_date != None:
        dff = df.loc[start_date:end_date, value]
    else:
        dff = df[value]
    fig = px.line(dff, x=dff.index, y=dff.columns)
    fig.update_layout(xaxis_title = 'Timestamp', yaxis_title = unit)
    return fig

def clean_df(df : pd.DataFrame, organized_mapping):
    for key, value in organized_mapping.items():
        fields = value["y1_fields"] + value["y2_fields"]

        # Iterate over the values and add traces to the figure
        for field_dict in fields:
            column_name = field_dict["column_name"]
            if 'lower_bound' in field_dict:
                df[column_name] = np.where(df[column_name] < field_dict["lower_bound"], np.nan, df[column_name])

            if 'upper_bound' in field_dict:
                df[column_name] = np.where(df[column_name] > field_dict["upper_bound"], np.nan, df[column_name])

def create_graph(dm : DataManager, graph_type : str, unique_group : str = None, cop_value : str = None):
    # start_time = time.time()
    return_value = "Graph type not recognized"
    if graph_type == 'raw_data':
        graph = RawDataSubPlots(dm)
        return_value = graph.get_graph()
    elif graph_type == 'hourly_shapes':
        graph = HourlyShapesPlots(dm)
        return_value = graph.get_graph()
    elif graph_type == "summary_bar_graph":
        summary_bar_graph = SummaryBarGraph(dm, summary_group=unique_group)
        return_value = summary_bar_graph.get_graph()
    # Hourly Power Graph
    elif graph_type == "summary_hour_graph":
        summary_hour_graph = SummaryDailyPowerByHour(dm, summary_group=unique_group)
        return_value = summary_hour_graph.get_graph()
    # Pie Graph
    elif graph_type == "summary_pie_chart":
        summary_pie_chart = SummaryPieGraph(dm, summary_group=unique_group)
        return_value = summary_pie_chart.get_graph()
    elif graph_type == "summary_gpdpp_histogram":
        summary_gpdpp_histogram = GPDPPHistogram(dm, summary_group=unique_group)
        return_value = summary_gpdpp_histogram.get_graph()
    # GPDPP Timeseries
    elif graph_type == 'summary_gpdpp_timeseries':
        summary_gpdpp_timeseries = GPDPPTimeseries(dm, summary_group=unique_group)
        return_value = summary_gpdpp_timeseries.get_graph()
    # Peak Norm Scatter
    elif graph_type == 'summary_peaknorm':
        summary_peaknorm = PeakNorm(dm, summary_group=unique_group)
        return_value = summary_peaknorm.get_graph()
    # Hourly Flow Percentiles
    elif graph_type == 'summary_hourly_flow':
        summary_hourly_flow = SummaryHourlyFlow(dm, summary_group=unique_group)
        return_value = summary_hourly_flow.get_graph()
    # COP Regression
    elif graph_type == 'summary_cop_regression':
        summary_cop_regression = COPRegression(dm, summary_group=unique_group, cop_column=cop_value)
        return_value = summary_cop_regression.get_graph()
    # COP Timeseries
    elif graph_type == 'summary_cop_timeseries':
        summary_cop_timeseries = COPTimeseries(dm, summary_group=unique_group)
        return_value = summary_cop_timeseries.get_graph()
    # DHW Box and Whisker
    elif graph_type == 'summary_flow_boxwhisker':
        summary_flow_boxwhisker = DHWBoxWhisker(dm, summary_group=unique_group)
        return_value = summary_flow_boxwhisker.get_graph()
    # ERV active vs passive hourly profile
    elif graph_type == 'summary_erv_performance':
        summary_erv_performance = ERVPerformance(dm, summary_group=unique_group)
        return_value = summary_erv_performance.get_graph()
    # OHP active vs passive hourly profile
    elif graph_type == 'summary_ohp_performance':
        summary_ohp_performance = OHPPerformance(dm, summary_group=unique_group)
        return_value = summary_ohp_performance.get_graph()
    # SERA office summary
    elif graph_type == 'summary_SERA_pie':
        summary_SERA_pie = SERAPie(dm, summary_group=unique_group)
        return_value = summary_SERA_pie.get_graph()
    # SERA monthly energy consumption
    elif graph_type == 'summary_SERA_monthly':
        summary_SERA_monthly = SERAMonthly(dm, summary_group=unique_group)
        return_value = summary_SERA_monthly.get_graph()
    # end_time = time.time()
    # elapsed_time = end_time - start_time
    # print(f"graphing {graph_type} took {elapsed_time:.4f} seconds to run.")
    return return_value

def create_summary_graphs(dm : DataManager):

    graph_components = []
    graph_components = dm.add_default_date_message(graph_components)
    unique_groups = dm.get_summary_groups()
    graph_types = ["summary_bar_graph", "summary_hour_graph", "summary_pie_chart", "summary_gpdpp_histogram",
                    'summary_gpdpp_timeseries', 'summary_peaknorm', 'summary_hourly_flow', 'summary_cop_regression',
                    'summary_cop_timeseries', 'summary_flow_boxwhisker', 'summary_erv_performance', 'summary_ohp_performance',
                    'summary_SERA_pie', 'summary_SERA_monthly']
    for unique_group in unique_groups:
        # Title if multiple groups:
        if len(unique_groups) > 1:
            graph_components.append(html.H2(unique_group))
        for graph_type in graph_types:
            if dm.graph_available(graph_type):
                graph_components.append(create_graph(dm, graph_type, unique_group))
                if graph_type == 'summary_cop_regression':
                    if not dm.sys_cop_variable_2 is None:
                        graph_components.append(create_graph(dm, graph_type, unique_group, cop_value=dm.sys_cop_variable_2))
                    if not dm.sys_cop_variable_3 is None:
                        graph_components.append(create_graph(dm, graph_type, unique_group, cop_value=dm.sys_cop_variable_3))
                    if not dm.sys_cop_variable_4 is None:
                        graph_components.append(create_graph(dm, graph_type, unique_group, cop_value=dm.sys_cop_variable_4))
    return graph_components
    

