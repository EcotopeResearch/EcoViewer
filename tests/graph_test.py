import pytest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import datetime
from ecoviewer.display.graphutils import *
from ecoviewer.objects.DataManager import DataManager
from ecoviewer.objects.GraphObject import GraphObject, graphs
import numpy as np
from datetime import datetime
from pandas.testing import assert_frame_equal
from dash import dcc, html

test_raw_df = pd.DataFrame({
                'Temp_DHW_Supply': [120,120,122.3],
                'PowerIn_HPWH': [1.0,1.5,0.3],
                'Flow_DHW': [4,5.3,2.7],
                'Temp_CityWater': [40,45,42.3]
                })
test_raw_df.index = pd.to_datetime(['2023-07-11 10:00:00', '2023-07-11 10:01:00','2023-07-11 10:02:00'])
test_organized_mapping = {
                'flow' : {
                    "title" : 'Flow',
                    "y1_units" : 'gpm',
                    "y2_units" : '',
                    "y1_fields" : [{'readable_name' :'Flow_DHW',
                                    'color' : 'red',
                                    'column_name' :'Flow_DHW',
                                    'description' :'Flow_DHW',
                                    'lower_bound' : 0,
                                    'upper_bound' :100}],
                    "y2_fields" : [],
                },
                'temp' : {
                    "title" : 'Temperature',
                    "y1_units" : 'F',
                    "y2_units" : '',
                    "y1_fields" : [{'readable_name' :'Temp_DHW_Supply',
                                    'color' : 'green',
                                    'column_name' :'Temp_DHW_Supply',
                                    'description' :'Temp_DHW_Supply'},
                                    {'readable_name' :'Temp_CityWater',
                                    'column_name' :'Temp_CityWater',
                                    'color' : 'blue',
                                    'description' :'Temp_CityWater'}],
                    "y2_fields" : [],
                },
                'power' : {
                    "title" : 'Power',
                    "y2_units" : 'kW',
                    "y1_units" : '',
                    "y2_fields" : [{'readable_name' :'PowerIn_HPWH',
                                    'column_name' :'PowerIn_HPWH',
                                    'color' : 'yellow',
                                    'description' :'PowerIn_HPWH'}],
                    "y1_fields" : [],
                }
            }

@patch('ecoviewer.objects.DataManager')
def test_unknown_graph_type(mock_data_manager):
    return_value = create_graph(mock_data_manager, 'not_a_real_graph_type')
    assert return_value == "Graph type not recognized"

@patch('ecoviewer.objects.DataManager')
def test_raw_data_graph(mock_data_manager):
    mock_data_manager.add_default_date_message.return_value = [html.P(style={'color': 'red', 'textAlign': 'center'}, children=[
        html.Br(),
        "No data available for date range selected. Defaulting to most recent data."
    ])]
    mock_data_manager.is_within_raw_data_limit.return_value = True
    mock_data_manager.get_raw_data_df.return_value = [test_raw_df.copy(), test_organized_mapping]
    mock_data_manager.value_in_checkbox_selection.return_value = True
    mock_data_manager.get_site_events.return_value = pd.DataFrame()
    ret_value = create_graph(mock_data_manager, 'raw_data')
    assert isinstance(ret_value, list)
    assert len(ret_value) == 2
    assert isinstance(ret_value[0], html.P)
    assert ret_value[0].style == {'color': 'red', 'textAlign': 'center'}
    assert isinstance(ret_value[0].children, list)
    assert isinstance(ret_value[0].children[0], html.Br)
    assert ret_value[0].children[1] == "No data available for date range selected. Defaulting to most recent data."
    assert isinstance(ret_value[1], dcc.Graph)

@pytest.mark.parametrize("graph_type, pretty_name, err_msg", [
    ('raw_data','Raw Data Plots','No data available for parameters specified.'),
    ('hourly_shapes','Hourly Shapes Plots','No data available for parameters specified.'),
    ('summary_cop_regression','COP Regression','No outdoor air temperature data available.'),
    ('summary_cop_timeseries','System COP Timeseries','No outdoor air temperature data available.'),
    ('summary_bar_graph','Energy and COP Bar Graph','No power or COP data to display for time period.')
])
def test_graph_no_data(graph_type, pretty_name, err_msg):
    with patch('ecoviewer.objects.DataManager') as mock_data_manager:
        mock_data_manager.oat_variable = "OAT"
        mock_data_manager.add_default_date_message.return_value = []
        mock_data_manager.is_within_raw_data_limit.return_value = True
        mock_data_manager.get_raw_data_df.return_value = pd.DataFrame(), {}
        mock_data_manager.get_daily_data_df.return_value = pd.DataFrame()
        mock_data_manager.get_daily_summary_data_df.return_value = pd.DataFrame()
        mock_data_manager.value_in_checkbox_selection.return_value = True
        ret_value = create_graph(mock_data_manager, graph_type)
        assert isinstance(ret_value, html.P)
        assert ret_value.style == {'color': 'red', 'textAlign': 'center'}
        assert isinstance(ret_value.children, list)
        assert len(ret_value.children) == 2
        assert isinstance(ret_value.children[0], html.Br)
        assert ret_value.children[1] == f"Could not generate {pretty_name}: {err_msg}"

@pytest.mark.parametrize("graph_type", [
    ("summary_SERA_pie"),('summary_SERA_monthly')
])
def test_load_default_pkl(graph_type):
    with patch('ecoviewer.objects.DataManager') as mock_data_manager:
        mock_data_manager.pkl_folder_path = 'path/does/not/matter/'
        mock_data_manager.selected_table = 'fake_site'
        with patch('os.path.isfile', return_value=True):
            with patch('pickle.load', return_value='great job!'):
                open_mock = mock_open(read_data='mocked file content')
                with patch('builtins.open', open_mock):
                    ret_value = create_graph(mock_data_manager, graph_type)
                    assert ret_value == 'great job!'
                    open_mock.assert_called_once_with(f'path/does/not/matter/fake_site_{graph_type}.pkl', "rb")



