import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from xml.etree import ElementTree as ET
from unittest.mock import patch, Mock, MagicMock 
import pytest 
import requests
import authenticate
import environment


class TestAuthenticate(unittest.TestCase):

    @patch('requests.post')
    @patch('requests.raise_for_status')
    def test_authenticate_tooling_api(self, mock_post, mock_raise_for_status):
        mock_raise_for_status.return_value = None
        username = 'username'
        password = 'password'
        token = 'token'
        mock_response = MagicMock()
        mock_response.return_value = 'test'
        mock_post.return_value = MockResponse('test')
        response = authenticate_tooling_api(username, password, token)

        self.assertEqual(response, 'test')
   

