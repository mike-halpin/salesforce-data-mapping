import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import query
import dataframe
from simple_salesforce import format_soql

def retrieve_all_object_records(object_name, fields_list):
    query_string = f'SELECT {",".join(fields_list)} FROM {object_name}'
    df = query.run_query(format_soql(query_string))
    if df.empty:
        print(f'No records found for {object_name}')

    return df


def main():
    object_ids_to_object_names = query.match_custom_object_ids_to_object_names()
    object_to_fields = query.match_custom_object_ids_to_field_names()
    object_ids = object_to_fields.keys()
    result_to_return = pd.DataFrame()
    for object_id in object_ids:
        fields = object_to_fields[object_id]
        fields.append('CreatedDate')
        object_name = object_ids_to_object_names[object_id]
        df = retrieve_all_object_records(object_name, fields)
        if df.empty:
            continue
        counts = dataframe.get_record_counts(df)

        why can't i loop fields and get the non null values for each field order by created date DESC LMIT 1??'

        most_recent_dates = dataframe.get_dates_of_last_created_non_null_values_for_each_field(df.loc[:, fields])
        most_common_values = dataframe.get_the_n_most_frequent_values_for_each_field(df.loc[:, fields], 3)

    for object_name in query.get_custom_object_names():
        description = query.query_field_descriptions_by_object(object_name)
        data_types = dataframe.get_data_types(description)

if __name__ == "__main__":
    main()

