import os
import unittest
from unittest.mock import patch, Mock
import salesforce.query as query

class TestGetCountOfSobjectFields(unittest.TestCase):
    def setUp(self):
        # Create a mock access token
        self.mock_access_token = 'mock_access_token'

        # Set the mock access token as an environment variable
        os.environ['SALESFORCE_ACCESS_TOKEN'] = self.mock_access_token

        # Mock endpoint
        self.mock_endpoint = 'https://mock.salesforce.com'

        # Mock query
        self.mock_query = 'SELECT Id, DeveloperName, NamespacePrefix FROM CustomObject'

        # Mock records
        self.mock_records = [
            {
                'Id': 'mock_id_1',
                'DeveloperName': 'mock_developer_name_1',
                'NamespacePrefix': 'mock_namespace_prefix_1'
            },
            {
                'Id': 'mock_id_2',
                'DeveloperName': 'mock_developer_name_2',
                'NamespacePrefix': 'mock_namespace_prefix_2'
            }
        ]

        # Mock response
        self.mock_response = Mock()
        self.mock_response.status_code = 200
        self.mock_response.json.return_value = {'records': self.mock_records}

    @patch('utilities.query.requests.get')
    @patch('utilities.dataframe.convert_records_to_dataframe')
    def test_query_custom_objects_details(self, mock_convert_records_to_dataframe, mock_get):
        # Set up mock functions
        mock_get.return_value = self.mock_response
        mock_convert_records_to_dataframe.return_value = self.mock_records

        # Call the function
        result = query.query_custom_objects_names()

        # Assert that the mock functions were called correctly
        mock_get.assert_called_once_with(self.mock_endpoint, headers={'Authorization': 'Bearer ' + self.mock_access_token, 'Content-Type': 'application/json'}, params={'q': self.mock_query})
        mock_convert_records_to_dataframe.assert_called_once_with(self.mock_records)

        # Assert that the result is correct
        self.assertEqual(result, self.mock_records)

    @patch('utilities.query.requests.get')
    def test_query_tooling_api(self, mock_get):
        # Set up the mock function
        mock_get.return_value = self.mock_response

        # Call the function
        result = query.query_tooling_api(self.mock_endpoint, self.mock_access_token, self.mock_query)

        # Assert that the mock function was called correctly
        mock_get.assert_called_once_with(self.mock_endpoint, headers={'Authorization': 'Bearer ' + self.mock_access_token, 'Content-Type': 'application/json'}, params={'q': self.mock_query})

        # Assert that the result is correct
        self.assertEqual(result, self.mock_records)

# Test other functions similarly

if __name__ == '__main__':
    unittest.main()

