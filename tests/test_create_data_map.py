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
GET_SALESFORCE_QUERY_PATCH = 'create_data_map.query.environment'
AUTHENTICATE_API_FUNCTION_PATCH = 'create_data_map.query.authenticate.authenticate_api'
EXTRACT_FIELDS_FROM_QUERY_STRING_PATCH = 'create_data_map.extract_fields_from_query_string'
REQUESTS_GET_PATCH = 'create_data_map.query.requests.get'
SOQL_LAST_CREATED_WITH_FIELD_PATCH = 'create_data_map.soql.soql_last_created_with_field'
SOQL_MOST_COMMON_VALUES_PATCH = 'create_data_map.soql.soql_most_common_values'
SOQL_COUNT_OF_FIELD_VALUE_PATCH = 'create_data_map.soql.get_count_of_field_value'
SOQL_COUNT_OF_FIELDS_VALUE_PATCH = 'create_data_map.soql.get_count_of_fields_values'



@pytest.fixture
def bad_status_code():
    return 400

@pytest.fixture
def good_status_code():
    return 200

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
def invalid_field_error_code():
    error_codes = 'INVALID_FIELD'
    return error_codes

@pytest.fixture
def malformed_query_error_code():
    error_code = 'MALFORMED_QUERY'
    return error_code

@pytest.fixture
def invalid_type_error_code():
    error_code = 'INVALID_TYPE'
    return error_code

@pytest.fixture
def malformed_query_kit():
    """Used to match the malformed query error messages of all varieties."""
    return {
            'message': "SELECT ,    FROM hed__Facility__c\n          ^\nERROR at Row:1:Column:11\nunexpected token: 'FROM'",
            'regex': r'(?P<command>[A-Z]+)\s+(?P<fields>[\w\(\)]+),?\s*FROM\s+(?P<table>[a-zA-Z_]+)\s+\^\s+ERROR at Row:(?P<row>\d+):Column:(?P<column>\d+)\n(?P<error>.+)'
            }

@pytest.fixture
def invalid_type_regex():
    """Used to match the Invalid Type error messages of all varieties."""
    return {
            'message': "sObject type 'hed__Facility__c' is not supported. If you are attempting to use a custom object, be sure to append the '__c' after the entity name. Please reference your WSDL or the describe call for the appropriate names.",
            'regex': r'sObject type \'(?P<object>[a-zA-Z_]+)\' is not supported.'
            }

@pytest.fixture
def invalid_field_regex():
    """Used to match the Invalid field error messages of all varieties."""
    return {
            'message': "\nSELECT COUNT(hed__Capacity__c), COUNT(hed__Description__c)\n             ^\nERROR at Row:1:Column:14\nInvalid field: 'hed__Capacity__c'",
            'regex': r'Invalid field: \'(?P<object>[a-zA-Z_]+)\''
            }

@pytest.fixture
def most_common_values():
    return {
            'message': "'SELECT hed__Course_Offering__c, COUNT(Id) FROM hed__Course_Offering_Schedule__c GROUP BY hed__Course_Offering__c ORDER BY COUNT(Id) DESC LIMIT 3'"
            }

@pytest.fixture
def most_recent_values():
    return {
            'message': 'SELECT CreatedDate, hed__Facility__c FROM hed__Course_Offering_Schedule__c WHERE hed__Facility__c != null ORDER BY CreatedDate DESC LIMIT 1'
            }

class MockParsedResponse(Mock):
    
    def __init__(self, 
                 export_dto=None, 
                 get_status_code=None, 
                 get_error_message=None, 
                 get_error_code=None, 
                 get_response_text=None, 
                 fields=None, 
                 is_error=None, 
                 is_object_error=None, 
                 is_field_error=None, 
                 is_query_error=None):

        super().__init__()
        self.export_dto = Mock(return_value=export_dto)
        self.get_status_code = Mock(return_value=get_status_code)
        self.get_error_message = Mock(return_value=get_error_message)
        self.get_error_code = Mock(return_value=get_error_code)
        self.get_response_text = Mock(return_value=get_response_text)
        self.fields = Mock(return_value=fields)
        self.is_error = Mock(return_value=is_error)
        self.is_object_error = Mock(return_value=is_object_error)
        self.is_field_error = Mock(return_value=is_field_error)
        self.is_query_error = Mock(return_value=is_query_error)

def get_mocked_parsedresponse_success(good_status_code):
     return MockParsedResponse(get_status_code=good_status_code)

def get_mocked_parsedresponse_calculate_counts_of_non_null_field_values(good_status_code, good_count_response):
    return MockParsedResponse(get_status_code=good_status_code, export_dto=good_count_response, is_error=False, is_object_error=False, is_field_error=False, is_query_error=False)

def get_mocked_parsedresponse_invalid_field(invalid_field_regex, bad_status_code, invalid_field_error_code):
    error_message = invalid_field_regex['message']
    return MockParsedResponse(get_status_code=bad_status_code, get_error_message=error_message, get_error_code=invalid_field_error_code)

def get_mocked_parsedresponse_invalid_type(invalid_type_regex, bad_status_code, invalid_type_error_code):
    error_message = invalid_type_regex['message']
    return MockParsedResponse(get_status_code=bad_status_code, get_error_message=error_message, get_error_code=invalid_type_error_code, is_error=True, is_object_error=False)

def get_mocked_parsedresponse_malformed_query(malformed_query_kit, bad_status_code, malformed_query_error_code):
    error_message = malformed_query_kit['message']
    error_code = malformed_query_error_code
    return MockParsedResponse(get_status_code=bad_status_code, get_error_message=error_message, get_error_code=error_code, is_error=True, is_query_error=True)

def get_mocked_parsedresponse_error(bad_status_code):
    return MockParsedResponse(get_status_code=bad_status_code)

def get_mocked_parsedresponse_most_common_values(most_common_values, good_status_code, good_most_common_response):
    return MockParsedResponse(get_status_code=good_status_code, export_dto=good_most_common_response, is_error=False)

def get_mocked_parsedresponse_most_recent_values(most_recent_values, good_status_code, good_most_recent_response):
    return MockParsedResponse(get_status_code=good_status_code, export_dto=good_most_recent_response, is_error=False)

@patch(PARSEDRESPONSE_PATCH)
# Tests for create_data_map.calculate_counts_of_non_null_field_values
def test_calculate_counts_of_non_null_field_values(mocked_parsed_response, good_count_response, good_status_code):
    mocked_parsed_response = get_mocked_parsedresponse_calculate_counts_of_non_null_field_values(good_status_code, good_count_response)
    fields = ['field0', 'field1', 'field2', 'field3', 'field4', 'field5']
    expected_result = {'field0': 0, 'field1': 1, 'field2': 2, 'field3': 3, 'field4': 4, 'field5': 5}

    result = create_data_map.calculate_counts_of_non_null_field_values(mocked_parsed_response, fields)
    
    # Verify the output of the function based on the provided mocked_parsed_response

    assert result == expected_result

# Tests for create_data_map.calculate_most_common_values_in_field
@patch(PARSEDRESPONSE_PATCH)
def test_calculate_most_common_values_in_field(mocked_parsed_response, good_most_common_response):
    # Use the fixture in the test
    mocked_parsed_response = get_mocked_parsedresponse_most_common_values(most_common_values, 200, good_most_common_response)
    result = create_data_map.calculate_most_common_values_in_field(mocked_parsed_response, 'hed__Contact__c')

    # Verify the output of the function based on the provided mocked_parsed_response
    expected_result = {'hed__Contact__c': [['0033m00002UhkaiAAB', 587], ['0033m00002RYnt1AAD', 315], ['0033m00002ZdhXWAAZ', 199]]}
    assert result == expected_result
    return True

# Tests for create_data_map.calculate_most_recent_created_date_in_field
@patch(PARSEDRESPONSE_PATCH)
def test_calculate_most_recent_created_date_in_field(mocked_parsed_response, good_created_date_response):
    # Use the fixture in the test
    mocked_parsed_response = get_mocked_parsedresponse_most_recent_values(most_recent_values, 200, good_created_date_response)
    result = create_data_map.calculate_most_recent_created_date_in_field(mocked_parsed_response)

    # Verify the output of the function based on the provided mocked_parsed_response
    expected_result = pd.to_datetime('2023-01-04T21:41:28.000+0000')
    assert result == expected_result


def test_save_datamap():
    # Given
    df = pd.DataFrame({
        'SObjectId': ['1', '1', '2', '3'],
        'FieldName': ['A', 'B', 'C', 'D']
        })
    object_id = '1'
    fields_and_values = {'A': 'Hello', 'B': 'World'}

    # When
    output_df = create_data_map.save_datamap(df, object_id, 'TEST', fields_and_values)
    logger.debug(output_df.head(5))
    # Then
    for field, value in fields_and_values.items():
        logger.debug(f'Checking {field} & {value} DataFrame[{field}] == {value}]')
        assert output_df.loc[(output_df['SObjectId'] == object_id) & (output_df['FieldName'] == field), 'TEST'].values[0] == value
    assert os.path.isfile('data_map.csv')  # also checks if the file was written successfully

    # Clean up
    os.remove('data_map.csv')
    return True


@patch(PARSEDRESPONSE_PATCH)
@patch(SOQL_COUNT_OF_FIELD_VALUE_PATCH)
def test_make_request_and_edit_query_until_success_with_field_error(mocked_parsed_response, mock_soql, invalid_field_regex, bad_status_code, invalid_field_error_code, good_fields, good_object): 
    mocked_parsed_response = get_mocked_parsedresponse_invalid_field(invalid_field_regex, bad_status_code, invalid_field_error_code)
    mock_soql.return_value = count_query_string
    result = create_data_map.make_request_and_edit_query_until_success(good_object, good_fields, mock_soql) 

    assert result == mock_response
    assert mock_run.call_count == 2
    mock_modify.assert_called_once_with("SELECT * FROM TABLE", 'field error', 1)


@patch(PARSEDRESPONSE_PATCH)
@patch(SOQL_COUNT_OF_FIELD_VALUE_PATCH)
@patch(AUTHENTICATE_API_FUNCTION_PATCH)
@patch(REQUESTS_GET_PATCH)
@patch(GET_SALESFORCE_QUERY_PATCH)
def test_make_request_and_edit_query_until_success_with_query_error(
    mocked_parsed_response, 
    mock_soql,
    mock_authenticate,
    mock_requests_get,
    mock_environment,
    count_query_string,
    malformed_query_kit,
    bad_status_code,
    malformed_query_error_code,
    good_object,
    good_fields
):
    mock_authenticate.return_value = ('session_id', 'https://server_url/services/data')
    mocked_parsed_response = get_mocked_parsedresponse_malformed_query(malformed_query_kit, bad_status_code, malformed_query_error_code)
    mock_soql.return_value = count_query_string 
    result = create_data_map.make_request_and_edit_query_until_success(good_object, good_fields, mock_soql)
    assert result['hasError'] == True
    

