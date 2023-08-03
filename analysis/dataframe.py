import pandas as pd

def convert_records_to_dataframe(records):
    df = pd.DataFrame(records)
    return df

def format_api_names_from_tooling_api(df):
    df.loc[df['NamespacePrefix'].notnull(), 'DeveloperName'] = df['NamespacePrefix'] + '__' + df['DeveloperName']
    df['DeveloperName'] = df['DeveloperName'] + '__c'
    return df

def get_record_counts(df):
    counts = df.count()
    #counts_df = pd.DataFrame(counts).T
    #counts_df.index = ['Record Count']
    #df = pd.concat([df, counts_df])
    return counts

def get_dates_of_last_created_non_null_values_for_each_field(df):
    df.set_index('CreatedDate', inplace=True)
    df.sort_index(ascending=False, inplace=True)  # So that the most recent dates are first
    most_recent_dates = df.apply(lambda col: col.first_valid_index())
    return most_recent_dates

def get_the_n_most_frequent_values_for_each_field(df, n):
    most_common_values = {col: df[col].value_counts().nlargest(3).index.tolist() for col in df.columns}
    return most_common_values

def get_data_types(df):
    return {field['name']: field['type'] for field in df}
