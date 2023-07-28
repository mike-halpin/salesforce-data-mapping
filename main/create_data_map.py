import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import re
import pandas as pd
from simple_salesforce import format_soql
import salesforce.api.query as query
import analysis.dataframe as dataframe
import salesforce.soql.soql as soql

def update_results(df, field, index, value):
    if isinstance(value, (list, tuple, set)):
        value = ', '.join(str(v) for v in value)
    try:
        df.loc[df['FieldName'] == field, index] = value
        pass
    except ValueError as e:
        pass
    return df

def make_request_and_edit_query_until_success(query_string):
    retry_limit = len(format.extract_fields_from_query_string(query_string)) - 1
    parsed_response = query.run_query_using_requests(query_string)
    while parsed_response['hasError'] and retry_limit > 0:
        retry_limit -= 1
        modified_query = query.modify_query_string(query_string, parsed_response['errorMessage'], parsed_response['errorCode'])
        parsed_response = query.run_query_using_requests(modified_query)

    return parsed_response

def get_results_to_return(object_ids_to_object_names, object_to_fields):
    pd_object_to_fields = pd.DataFrame(object_to_fields.items(), index=object_to_fields.keys(), columns=['SObjectId', 'FieldName']).explode('FieldName')
    object_ids = object_to_fields.keys()
    pd_object_ids_to_object_names = pd.DataFrame(object_ids_to_object_names.items(), index=object_ids_to_object_names.keys(), columns=['SObjectId', 'SObjectName'])
    results_to_return = pd_object_to_fields.merge(pd_object_ids_to_object_names, on='SObjectId', how='left')

    for object_id in object_ids:
        fields = object_to_fields[object_id]
        object_name = object_ids_to_object_names[object_id]
        counts_of_non_null_field_values = make_request_and_edit_query_until_success(soql.get_count_of_fields_values(object_name, fields))
        if counts_of_non_null_field_values['hasError']:
            continue
        else:
            count_values = list(counts_of_non_null_field_values['records'][0].values())[1:] if len(list(counts_of_non_null_field_values['records'][0].values())) > 0 else 0
            if sum(count_values) == 0:
                continue
            query_fields = re.findall(r'COUNT\((.*?)\)', counts_of_non_null_field_values['queryString'])
            for idx, field in enumerate(query_fields):
                results_to_return = update_results(results_to_return, field, 'Count', count_values[idx])

        for field in fields:
            most_common_values_in_field = query.run_query_using_requests(soql.get_most_common_values(object_name, field))
            if most_common_values_in_field['records']:
                most_common_values = set()
                for i in range(len(fields)):
                    key = 'expr' + str(i)
                    most_common_values.update(set([item[key] for item in most_common_values_in_field['records'] if key in item]))
                try:
                    results_to_return = update_results(results_to_return, field, 'MostCommonValues', most_common_values)
                except ValueError as e:
                    print(most_common_values)
            most_recent_created_date_in_field = query.run_query_using_requests(soql.get_last_created_with_field(object_name, field))
            if most_recent_created_date_in_field['records']:
                created_date = most_recent_created_date_in_field['records'][0]['CreatedDate']
                results_to_return = update_results(results_to_return, field, 'DateOfLastValue', created_date)

    return results_to_return

def main():
    try:
        object_ids_to_object_names = query.match_custom_object_ids_to_object_names()
        object_to_fields = query.match_custom_object_ids_to_field_names()
        results_to_return = get_results_to_return(object_ids_to_object_names, object_to_fields)

        results_to_return.to_csv('data_map.csv', index=False)

        for object_name in query.get_custom_object_names():
            description = query.query_field_descriptions_by_object(object_name)
            data_types = dataframe.get_data_types(description)
    except Exception as e:
        results_to_return.to_csv('data_map.csv', index=False)
        print(e)

if __name__ == "__main__":
    main()

