import os
from dotenv import load_dotenv

ENVIRONMENT_VARIABLES_FILE_NAME = '.env'

def load_environment_variables():
    current_path = os.path.dirname(os.path.realpath(__file__))
    env_path = os.path.join(current_path, ENVIRONMENT_VARIABLES_FILE_NAME)
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
    elif os.path.exists(ENVIRONMENT_VARIABLES_FILE_NAME):
        load_dotenv(dotenv_path=ENVIRONMENT_VARIABLES_FILE_NAME)
    else:
        raise ValueError('No .env file found')

    return

def get_salesforce_access_token():
    access_token = os.environ.get('SALESFORCE_ACCESS_TOKEN') or ''
    if access_token == '':
        raise ValueError('SALESFORCE_ACCESS_TOKEN environment variable not set')
    return access_token

def get_salesforce_password():
    salesforce_password = os.environ.get('SALESFORCE_PASSWORD') or ''
    if salesforce_password == '':
        raise ValueError('SALESFORCE_PASSWORD environment variable not set')
    return salesforce_password

def get_salesforce_url():
    salesforce_domain = os.environ.get('SALESFORCE_DOMAIN') or ''
    if salesforce_domain == '':
        raise ValueError('SALESFORCE_DOMAIN environment variable not set')
    log.info('Salesforce domain: ' + salesforce_domain)
    return salesforce_domain

def get_salesforce_username():
    salesforce_username = os.environ.get('SALESFORCE_USERNAME') or ''
    if salesforce_username == '':
        raise ValueError('SALESFORCE_USERNAME environment variable not set')
    return salesforce_username
