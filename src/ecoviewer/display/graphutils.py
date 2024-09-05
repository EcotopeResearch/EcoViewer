import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
import numpy as np
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

def create_date_note(site_name, cursor, pretty_name):
    """
    returns [date_note, first_date, last_date]
    """
    query = f"SELECT time_pt FROM {site_name} ORDER BY time_pt ASC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 0 or len(result[0]) == 0:
        return ""
    first_date = result[0][0]

    query = f"SELECT time_pt FROM {site_name} ORDER BY time_pt DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    last_date = result[0][0]

    return [
            f"Possible range for {pretty_name}:",
            html.Br(),
            f"{first_date.strftime('%m/%d/%y')} - {last_date.strftime('%m/%d/%y')}"
    ]

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

def create_graph(dm : DataManager, graph_type : str, unique_group : str = None):
    # print(graph_type)
    if graph_type == 'raw_data':
        graph = RawDataSubPlots(dm)
        return graph.get_graph()
    elif graph_type == 'hourly_shapes':
        graph = HourlyShapesPlots(dm)
        return graph.get_graph()
    if graph_type == "summary_bar_graph":
        summary_bar_graph = SummaryBarGraph(dm, summary_group=unique_group)
        return summary_bar_graph.get_graph()
    # Hourly Power Graph
    if graph_type == "summary_hour_graph":
        summary_hour_graph = SummaryDailyPowerByHour(dm, summary_group=unique_group)
        return summary_hour_graph.get_graph()
    # Pie Graph
    if graph_type == "summary_pie_chart":
        summary_pie_chart = SummaryPieGraph(dm, summary_group=unique_group)
        return summary_pie_chart.get_graph()
    if graph_type == "summary_gpdpp_histogram":
        summary_gpdpp_histogram = GPDPPHistogram(dm, summary_group=unique_group)
        return summary_gpdpp_histogram.get_graph()
    # GPDPP Timeseries
    if graph_type == 'summary_gpdpp_timeseries':
        summary_gpdpp_timeseries = GPDPPTimeseries(dm, summary_group=unique_group)
        return summary_gpdpp_timeseries.get_graph()
    # Peak Norm Scatter
    if graph_type == 'summary_peaknorm':
        summary_peaknorm = PeakNorm(dm, summary_group=unique_group)
        return summary_peaknorm.get_graph()
    # Hourly Flow Percentiles
    if graph_type == 'summary_hourly_flow':
        summary_hourly_flow = SummaryHourlyFlow(dm, summary_group=unique_group)
        return summary_hourly_flow.get_graph()
    # COP Regression
    if graph_type == 'summary_cop_regression':
        summary_cop_regression = COPRegression(dm, summary_group=unique_group)
        return summary_cop_regression.get_graph()
    # COP Timeseries
    if graph_type == 'summary_cop_timeseries':
        summary_cop_timeseries = COPTimeseries(dm, summary_group=unique_group)
        return summary_cop_timeseries.get_graph()
    # DHW Box and Whisker
    if graph_type == 'summary_flow_boxwhisker':
        summary_flow_boxwhisker = DHWBoxWhisker(dm, summary_group=unique_group)
        return summary_flow_boxwhisker.get_graph()
    # ERV active vs passive hourly profile
    if graph_type == 'summary_erv_performance':
        summary_erv_performance = ERVPerformance(dm, summary_group=unique_group)
        return summary_erv_performance.get_graph()
    # OHP active vs passive hourly profile
    if graph_type == 'summary_ohp_performance':
        summary_ohp_performance = OHPPerformance(dm, summary_group=unique_group)
        return summary_ohp_performance.get_graph()
    # SERA office summary
    if graph_type == 'summary_SERA_pie':
        summary_SERA_pie = SERAPie(dm, summary_group=unique_group)
        return summary_SERA_pie.get_graph()
    # SERA monthly energy consumption
    if graph_type == 'summary_SERA_monthly':
        summary_SERA_monthly = SERAMonthly(dm, summary_group=unique_group)
        return summary_SERA_monthly.get_graph()
    return "Graph type not recognized"

def create_summary_graphs(dm : DataManager):

    graph_components = []
    graph_components = dm.add_default_date_message(graph_components)
    unique_groups = dm.get_summary_groups()
    for unique_group in unique_groups:
        # Title if multiple groups:
        if len(unique_groups) > 1:
            graph_components.append(html.H2(unique_group))
        # Bar Graph
        if dm.graph_available("summary_bar_graph"):
            graph_components.append(create_graph(dm,'summary_bar_graph',unique_group))
            # summary_bar_graph = SummaryBarGraph(dm, summary_group=unique_group)
            # graph_components.append(summary_bar_graph.get_graph())
        # Hourly Power Graph
        if dm.graph_available("summary_hour_graph"):
            # summary_hour_graph = SummaryDailyPowerByHour(dm, summary_group=unique_group)
            graph_components.append(create_graph(dm,'summary_hour_graph',unique_group))
        # Pie Graph
        if dm.graph_available("summary_pie_chart"):
            graph_components.append(create_graph(dm,'summary_pie_chart',unique_group))
            # summary_pie_chart = SummaryPieGraph(dm, summary_group=unique_group)
            # graph_components.append(summary_pie_chart.get_graph())
        if dm.graph_available("summary_gpdpp_histogram"):
            graph_components.append(create_graph(dm,'summary_gpdpp_histogram',unique_group))
            # summary_gpdpp_histogram = GPDPPHistogram(dm, summary_group=unique_group)
            # graph_components.append(summary_gpdpp_histogram.get_graph())
        # GPDPP Timeseries
        if dm.graph_available('summary_gpdpp_timeseries'):
            graph_components.append(create_graph(dm,'summary_gpdpp_timeseries',unique_group))
            # summary_gpdpp_timeseries = GPDPPTimeseries(dm, summary_group=unique_group)
            # graph_components.append(summary_gpdpp_timeseries.get_graph())
        # Peak Norm Scatter
        if dm.graph_available('summary_peaknorm'):
            graph_components.append(create_graph(dm,'summary_peaknorm',unique_group))
            # summary_peaknorm = PeakNorm(dm, summary_group=unique_group)
            # graph_components.append(summary_peaknorm.get_graph())
        # Hourly Flow Percentiles
        if dm.graph_available('summary_hourly_flow'):
            graph_components.append(create_graph(dm,'summary_hourly_flow',unique_group))
            # summary_hourly_flow = SummaryHourlyFlow(dm, summary_group=unique_group)
            # graph_components.append(summary_hourly_flow.get_graph())
        # COP Regression
        if dm.graph_available('summary_cop_regression'):
            graph_components.append(create_graph(dm,'summary_cop_regression',unique_group))
            # summary_cop_regression = COPRegression(dm, summary_group=unique_group)
            # graph_components.append(summary_cop_regression.get_graph())
        # COP Timeseries
        if dm.graph_available('summary_cop_timeseries'):
            graph_components.append(create_graph(dm,'summary_cop_timeseries',unique_group))
            # summary_cop_timeseries = COPTimeseries(dm, summary_group=unique_group)
            # graph_components.append(summary_cop_timeseries.get_graph())
        # DHW Box and Whisker
        if dm.graph_available('summary_flow_boxwhisker'):
            graph_components.append(create_graph(dm,'summary_flow_boxwhisker',unique_group))
            # summary_flow_boxwhisker = DHWBoxWhisker(dm, summary_group=unique_group)
            # graph_components.append(summary_flow_boxwhisker.get_graph())
        # ERV active vs passive hourly profile
        if dm.graph_available('summary_erv_performance'):
            graph_components.append(create_graph(dm,'summary_erv_performance',unique_group))
            # summary_erv_performance = ERVPerformance(dm, summary_group=unique_group)
            # graph_components.append(summary_erv_performance.get_graph())
        # OHP active vs passive hourly profile
        if dm.graph_available('summary_ohp_performance'):
            graph_components.append(create_graph(dm,'summary_ohp_performance',unique_group))
            # summary_ohp_performance = OHPPerformance(dm, summary_group=unique_group)
            # graph_components.append(summary_ohp_performance.get_graph())
        # SERA office summary
        if dm.graph_available('summary_SERA_pie'):
            graph_components.append(create_graph(dm,'summary_SERA_pie',unique_group))
            # summary_SERA_pie = SERAPie(dm, summary_group=unique_group)
            # graph_components.append(summary_SERA_pie.get_graph())
        # SERA monthly energy consumption
        if dm.graph_available('summary_SERA_monthly'):
            graph_components.append(create_graph(dm,'summary_SERA_monthly',unique_group))
            # summary_SERA_monthly = SERAMonthly(dm, summary_group=unique_group)
            # graph_components.append(summary_SERA_monthly.get_graph())
    return graph_components

def bayview_power_processing(df : pd.DataFrame) -> pd.DataFrame:
    df['PowerIn_SwingTank'] = df['PowerIn_ERTank1'] + df['PowerIn_ERTank2'] + df['PowerIn_ERTank5'] + df['PowerIn_ERTank6']

    # Drop the 'PowerIn_ER#' columns
    df = df.drop(['PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6'], axis=1)
    return df

def bayview_prune_additional_power(df : pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = ['PowerIn_Swing', 'PowerIn_ERTank1', 'PowerIn_ERTank2', 'PowerIn_ERTank5', 'PowerIn_ERTank6', 'PowerIn_HPWH']
    columns_to_drop = [col for col in df.columns if col.startswith("PowerIn_") and col not in columns_to_keep]
    df = df.drop(columns=columns_to_drop)
    return df
    

