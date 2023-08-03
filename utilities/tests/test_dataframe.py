import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal
import dataframe  # replace with the actual module name

def test_convert_records_to_dataframe():
    records = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 40}]
    expected_df = pd.DataFrame(records)
    result_df = dataframe.convert_records_to_dataframe(records)
    assert_frame_equal(result_df, expected_df)

def test_format_api_names_from_tooling_api():
    df = pd.DataFrame({
        'DeveloperName': ['TestName', 'AnotherName'],
        'NamespacePrefix': [None, 'Namespace']
    })
    expected_df = pd.DataFrame({
        'DeveloperName': ['TestName__c', 'Namespace__AnotherName__c'],
        'NamespacePrefix': [None, 'Namespace']
    })
    result_df = dataframe.format_api_names_from_tooling_api(df)
    assert_frame_equal(result_df, expected_df)

def test_get_record_counts():
    df = pd.DataFrame({
        'Name': ['John', 'Jane', None],
        'Age': [30, 40, 50]
    })
    expected_counts = pd.Series({'Name': 2, 'Age': 3})
    result_counts = dataframe.get_record_counts(df)
    pd.testing.assert_series_equal(result_counts, expected_counts)

def test_get_dates_of_last_created_non_null_values_for_each_field():
    df = pd.DataFrame({
        'CreatedDate': ['2022-12-01', '2022-12-02', '2022-12-03'],
        'Value': [None, 2, 3]
    })
    expected_dates = pd.Series(['2022-12-03'], index=['Value'])
    result_dates = dataframe.get_dates_of_last_created_non_null_values_for_each_field(df)
    pd.testing.assert_series_equal(result_dates, expected_dates)

def test_get_the_n_most_frequent_values_for_each_field():
    df = pd.DataFrame({
        'Name': ['John', 'Jane', 'John', 'John', 'Jane', 'Jane', 'John'],
    })
    expected_values = {'Name': ['John', 'Jane']}
    result_values = dataframe.get_the_n_most_frequent_values_for_each_field(df, 2)
    assert result_values == expected_values

def test_get_data_types():
    df = pd.DataFrame({
        'Name': pd.Series(dtype='object'),
        'Age': pd.Series(dtype='int64'),
    })
    expected_types = {'Name': 'object', 'Age': 'int64'}
    result_types = dataframe.get_data_types(df)
    assert result_types == expected_types

