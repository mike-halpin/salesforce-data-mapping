import re


def format_api_names_from_tooling_api(df):
    df.loc[df['NamespacePrefix'].notnull(), 'DeveloperName'] = df['NamespacePrefix'] + '__' + df['DeveloperName']
    df['DeveloperName'] = df['DeveloperName'] + '__c'
    return df


def extract_fields_from_query_string(query_string: str) -> list:
    # find the contents between 'SELECT' and 'FROM', and remove leading/trailing whitespaces
    fields_str = re.search('SELECT(.+?)FROM', query_string, re.IGNORECASE).group(1).strip()

    # split the fields by comma and remove leading/trailing whitespaces for each field
    fields = [field.strip() for field in fields_str.split(',')]

    return fields
