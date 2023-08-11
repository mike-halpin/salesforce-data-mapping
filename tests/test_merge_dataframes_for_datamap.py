import os
import sys
import unittest
import pandas as pd
import pytest
from create_data_map import merge_dataframes_for_datamap

@pytest.fixture
def test_count_of_non_null_field_values():
    return [
    {"FieldName":"slackv2__Job_Frequency__c","CountOfNonNullValues":190},
    {"FieldName":"slackv2__Job_Id__c","CountOfNonNullValues":None},
    {"FieldName":"slackv2__Job_Start_Hour__c","CountOfNonNullValues":0}
    ]
