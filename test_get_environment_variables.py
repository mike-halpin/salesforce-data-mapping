import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from unittest.mock import patch, Mock, MagicMock
import pytest
import environment

ENVIRONMENT_VARIABLES_FILE_NAME = os.path.join('.env')

def create_env_file(username=False, password=False, access_token=False, domain=False):
    with open(ENVIRONMENT_VARIABLES_FILE_NAME, 'w') as f:
        if username:
            f.write('SALESFORCE_USERNAME=test\n')
        if password:
            f.write('SALESFORCE_PASSWORD=test\n')
        if access_token:
            f.write('SALESFORCE_ACCESS_TOKEN=test\n')
        if domain:
            f.write('SALESFORCE_DOMAIN=test\n')
    return


def delete_env_file():
    os.environ.clear()  # Clear the environment variables
    os.remove(ENVIRONMENT_VARIABLES_FILE_NAME)  # Delete the file

class TestEnvironmentVariables:
    def test_load_environment_variables(self):
        create_env_file(username=True, password=True, access_token=True, domain=True)
        environment.load_environment_variables()
        assert os.environ.get('SALESFORCE_USERNAME')
        assert os.environ.get('SALESFORCE_PASSWORD')
        assert os.environ.get('SALESFORCE_ACCESS_TOKEN')
        assert os.environ.get('SALESFORCE_DOMAIN')
        delete_env_file()
        return

    def test_get_salesforce_username(self):
        with patch.dict(os.environ, {'SALESFORCE_USERNAME': ''}):
            with pytest.raises(ValueError):
               environment.get_salesforce_username()
        with patch.dict(os.environ, {'SALESFORCE_USERNAME': 'username'}):
            assert environment.get_salesforce_username() == 'username'
        return

    def test_get_salesforce_password(self):
        with patch.dict(os.environ, {'SALESFORCE_PASSWORD': ''}):
            with pytest.raises(ValueError):
                environment.get_salesforce_password()

        with patch.dict(os.environ, {'SALESFORCE_PASSWORD': 'password'}):
            assert environment.get_salesforce_password() == 'password'
        return

    def test_get_salesforce_access_token(self):
        with patch.dict(os.environ, {'SALESFORCE_ACCESS_TOKEN': ''}):
            with pytest.raises(ValueError):
                environment.get_salesforce_access_token()

        with patch.dict(os.environ, {'SALESFORCE_ACCESS_TOKEN': 'token'}):
            assert environment.get_salesforce_access_token() == 'token'
        return
