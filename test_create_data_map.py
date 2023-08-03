import os
import sys
print(sys.path)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from .. import create_data_map 
import salesforce.log_config as log_config
logger = log_config.get_logger(__name__)

CALCULATE_MOST_COMMON_VALUES_IN_FIELD_PATCH = 'create_data_map.calculate_most_common_values_in_field'
CALCULATE_MOST_RECENT_CREATED_DATE_IN_FIELD_PATCH = 'create_data_map.calculate_most_recent_created_date_in_field'
CALCULATE_COUNTS_OF_NON_NULL_FIELD_VALUES_PATCH = 'create_data_map.calculate_counts_of_non_null_field_values'
SAVE_DATAMAP_PATCH = 'create_data_map.save_datamap'
MAKE_REQUEST_AND_EDIT_QUERY_UNTIL_SUCCESS_PATCH = 'create_data_map.make_request_and_edit_query_until_success'
RUN_QUERY_USING_REQUESTS_PATCH = 'create_data_map.run_query_using_requests'
MODIFY_QUERY_STRING_PATCH = 'create_data_map.removed_errored_fields'
PARSEDRESPONSE_PATCH = 'create_data_map.query.ParsedResponse.export_dto'
AUTHENTICATE_API_FUNCTION_PATCH = 'create_data_map.authenticate_api'
EXTRACT_FIELDS_FROM_QUERY_STRING_PATCH = 'create_data_map.extract_fields_from_query_string'
REQUESTS_GET_PATCH = 'create_data_map.requests.get'

# Fixture for a sample DataFrame for create_data_map.save_datamap testing @pytest.fixture def sample_dataframe(): data = {'FieldName': ['Field1', 'Field2', 'Field3'], 'CountOfNonNullValues': [0, 0, 0]} return pd.DataFrame(data)
@pytest.fixture
def good_fields():
    fields = ['Answer__c', 'Question__c', 'Article_Body__c', 'Answer__c', 'Question__c', 'Article_Body__c', 'Answer__c',
              'Question__c', 'Article_Body__c']
    return fields

@pytest.fixture
def good_object():
    object_name = 'Knowledge__c'
    return object_name

@pytest.fixture
def count_query_string(good_fields):
    count_query_string = f'SELECT {", ".join([f"COUNT({field_name})" for field_name in good_fields])} FROM FOO__c'
    return count_query_string

@pytest.fixture
def good_count_response():
    response = {"totalSize": 1, "done": True, "records": [
        {"attributes": {"type": "AggregateResult"}, "expr0": 0, "expr1": 1, "expr2": 2, "expr3": 3, "expr4": 4,
         "expr5": 5}]}
    return response

@pytest.fixture
def good_count_response_text(good_count_response):
    response_text = str(good_count_response)
    return response_text

@pytest.fixture
def good_most_common_response():
    response = {"records": [{'attributes': {'type': 'AggregateResult'}, 'hed__Contact__c': '0033m00002UhkaiAAB', 'expr0': 587}, {'attributes': {'type': 'AggregateResult'}, 'hed__Contact__c': '0033m00002RYnt1AAD', 'expr0': 315}, {'attributes': {'type': 'AggregateResult'}, 'hed__Contact__c': '0033m00002ZdhXWAAZ', 'expr0': 199}]}
    return response

@pytest.fixture
def good_created_date_response():
    response = {"records": [{'attributes': {'type': 'slackv2__Subscription_Condition__c', 'url': '/services/data/v57.0/sobjects/slackv2__Subscription_Condition__c/a1h3m00000FkroCAAR'}, 'CreatedDate': '2023-01-04T21:41:28.000+0000', 'slackv2__Subscription_Type__c': 'a1j3m000003qhcbAAA'}]}
    return response

@pytest.fixture
def good_record_counts(good_count_response):
    records = good_count_response['records']
    return records

@pytest.fixture
def error_codes():
    error_codes = ['MALFORMED_QUERY', 'INVALID_TYPE', 'INVALID_FIELD']
    return error_codes

@pytest.fixture
def get_malformed_query_kit():
    """Used to match the malformed query error messages of all varieties."""
    return {
            'message': "SELECT ,    FROM hed__Facility__c\n          ^\nERROR at Row:1:Column:11\nunexpected token: 'FROM'",
            'regex': r'(?P<command>[A-Z]+)\s+(?P<fields>[\w\(\)]+),?\s*FROM\s+(?P<table>[a-zA-Z_]+)\s+\^\s+ERROR at Row:(?P<row>\d+):Column:(?P<column>\d+)\n(?P<error>.+)'
            }

@pytest.fixture
def get_invalid_type_regex():
    """Used to match the Invalid Type error messages of all varieties."""
    return {
            'message': "sObject type 'hed__Facility__c' is not supported. If you are attempting to use a custom object, be sure to append the '__c' after the entity name. Please reference your WSDL or the describe call for the appropriate names.",
            'regex': r'sObject type \'(?P<object>[a-zA-Z_]+)\' is not supported.'
            }

@pytest.fixture
def get_invalid_field_regex():
    """Used to match the Invalid field error messages of all varieties."""
    return {
            'message': "\nSELECT COUNT(hed__Capacity__c), COUNT(hed__Description__c)\n             ^\nERROR at Row:1:Column:14\nInvalid field: 'hed__Capacity__c'",
            'regex': r'Invalid field: \'(?P<object>[a-zA-Z_]+)\''
            }

@pytest.fixture
def get_most_common_values():
    return {
            'message': "'SELECT hed__Course_Offering__c, COUNT(Id) FROM hed__Course_Offering_Schedule__c GROUP BY hed__Course_Offering__c ORDER BY COUNT(Id) DESC LIMIT 3'"
            }

@pytest.fixture
def get_most_recent_values():
    return {
            'message': 'SELECT CreatedDate, hed__Facility__c FROM hed__Course_Offering_Schedule__c WHERE hed__Facility__c != null ORDER BY CreatedDate DESC LIMIT 1'
            }

@patch(PARSEDRESPONSE_PATCH)
# Tests for create_data_map.calculate_counts_of_non_null_field_values
def test_calculate_counts_of_non_null_field_values(mocked_parsed_response, good_count_response):
    mocked_parsed_response.export_dto.return_value = good_count_response
    mocked_parsed_response.is_error.return_value = False
    fields = ['field0', 'field1', 'field2', 'field3', 'field4', 'field5']
    expected_result = {'field0': 0, 'field1': 1, 'field2': 2, 'field3': 3, 'field4': 4, 'field5': 5}

    result = create_data_map.calculate_counts_of_non_null_field_values(mocked_parsed_response, fields)
    
    # Verify the output of the function based on the provided mocked_parsed_response

    assert result == expected_result

# Tests for create_data_map.calculate_most_common_values_in_field
@patch(PARSEDRESPONSE_PATCH)
def test_calculate_most_common_values_in_field(mocked_parsed_response, good_most_common_response):
    # Use the fixture in the test
    mocked_parsed_response.export_dto.return_value = good_most_common_response
    result = create_data_map.calculate_most_common_values_in_field(mocked_parsed_response, 'hed__Contact__c')

    # Verify the output of the function based on the provided mocked_parsed_response
    expected_result = {'hed__Contact__c': [['0033m00002UhkaiAAB', 587], ['0033m00002RYnt1AAD', 315], ['0033m00002ZdhXWAAZ', 199]]}
    assert result == expected_result
    return True

# Tests for create_data_map.calculate_most_recent_created_date_in_field
@patch(PARSEDRESPONSE_PATCH)
def test_calculate_most_recent_created_date_in_field(mocked_parsed_response, good_created_date_response):
    # Use the fixture in the test
    mocked_parsed_response.export_dto.return_value = good_created_date_response
    result = create_data_map.calculate_most_recent_created_date_in_field(mocked_parsed_response)

    # Verify the output of the function based on the provided mocked_parsed_response
    expected_result = pd.to_datetime('2023-01-04T21:41:28.000+0000')
    assert result == expected_result


def test_save_datamap():
    # Given
    df = pd.DataFrame({
        'SObjectId': ['1', '1', '2', '3'],
        'FieldName': ['A', 'B', 'C', 'D'],
        'Value': [None, None, None, None]
        })
    object_id = '1'
    fields_values = {'A': 'Hello', 'B': 'World'}

    # When
    output_df = save_datamap(df, object_id, fields_values)

    # Then
    for field, value in fields_values.items():
        assert output_df.loc[(output_df['SObjectId'] == object_id) & (output_df['FieldName'] == field), 'Value'].values[0] == value
    assert os.path.isfile('data_map.csv')  # also checks if the file was written successfully

    # Clean up
    os.remove('data_map.csv')

@patch(EXTRACT_FIELDS_FROM_QUERY_STRING_PATCH, return_value=['field1', 'field2'])
@patch(RUN_QUERY_USING_REQUESTS_PATCH)
@patch(MODIFY_QUERY_STRING_PATCH)
def test_make_request_and_edit_query_until_success_no_error(mock_modify, mock_run, mock_extract):
    mock_response = Mock()
    mock_response.export_dto.return_value = {'hasError': False}
    mock_run.return_value = mock_response

    from create_data_map import make_request_and_edit_query_until_success
    result = make_request_and_edit_query_until_success("SELECT * FROM TABLE")

    assert result == mock_response
    mock_run.assert_called_once()
    mock_modify.assert_not_called()


@patch(EXTRACT_FIELDS_FROM_QUERY_STRING_PATCH, return_value=['field1', 'field2'])
@patch(RUN_QUERY_USING_REQUESTS_PATCH)
@patch(MODIFY_QUERY_STRING_PATCH)
def test_make_request_and_edit_query_until_success_with_field_error(mock_modify, mock_run, mock_extract):
    mock_response = Mock()
    mock_response.is_field_error.return_value = True
    mock_response.is_query_error.return_value = False
    mock_response.export_dto.side_effect = [{'hasError': True, 'errorMessage': 'field error', 'errorCode': 1},
                                            {'hasError': False}]

    mock_run.return_value = mock_response

    from create_data_map import make_request_and_edit_query_until_success
    result = make_request_and_edit_query_until_success("SELECT * FROM TABLE")

    assert result == mock_response
    assert mock_run.call_count == 2
    mock_modify.assert_called_once_with("SELECT * FROM TABLE", 'field error', 1)


@patch(PARSEDRESPONSE_PATCH)
@patch(EXTRACT_FIELDS_FROM_QUERY_STRING_PATCH, return_value=['field1', 'field2'])
@patch(RUN_QUERY_USING_REQUESTS_PATCH)
@patch(MODIFY_QUERY_STRING_PATCH)
def test_make_request_and_edit_query_until_success_with_query_error(mock_modify, mock_run, mock_extract, mock_parsed,
                                                                    count_query_string, good_count_response,
                                                                    get_malformed_query_kit):
    # mock mocked_parsed_response = query.run_query_using_requests(query_string)
    mock_response = Mock()
    mock_response.is_field_error.return_value = False
    mock_response.is_query_error.return_value = True
    mock_response.export_dto.return_value = {
            'records': good_record_counts,
            'statusCode': 400,
            'errorType': 'MALFORMED_QUERY',
            'errorMessage': get_malformed_query_kit['message'],
            'responseText': return_value['responseText'],
            'queryString': count_query_string,
            'errors': [{'errorCode': return_value['errorType'], 'message': return_value['errorMessage']}],
            'hasError': True if return_value['errorType'] or return_value['errorMessage'] else False
            }
    mock_run.return_value = mock_response

    # mock dto

    from create_data_map import make_request_and_edit_query_until_success
    result = make_request_and_edit_query_until_success("SELECT * FROM TABLE")

    assert result == mock_response
    assert mock_run.call_count == 1
    mock_modify.assert_not_called()
