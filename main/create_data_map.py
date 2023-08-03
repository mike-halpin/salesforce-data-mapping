import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import re
import pandas as pd
from simple_salesforce import format_soql
import analysis.dataframe as dataframe
import utilities.format as format
import salesforce.api.query as query
import salesforce.soql.soql as soql
import salesforce.log_config as log_config

# Initialize logging.
logger = log_config.get_logger(__name__)


def save_datamap_deprecated(df,object_id, fields, axis1_name, value):
    logger.debug('Deprecated save_datamap called for object_id: %s, fields: %s', object_id, fields)
    if isinstance(value, (list, tuple, set)):
        value = ', '.join(str(v) for v in value)
    try:
        df.loc[(df['SObjectId'] == object_id) & (df['FieldName'] == fields), axis1_name] = value
    except ValueError as e:
        logger.error('Field: ' + fields + ' Value: ' + value)
        logger.error(e)
    df.to_csv('data_map.csv', index=False)
    return df

def save_datamap(df, object_id, axis1_name, fields_and_values):
    # Logt
    logger.debug('Saving datamap for object_id: %s', object_id)
    logger.trace('Fields and values: %s', fields_and_values) 
    # Turn the updates into a DataFrame
    updates_df = pd.DataFrame.from_records([fields_and_values]).transpose().reset_index().rename(columns={'index': 'FieldName'})
    # Log
    logger.trace('Updates DataFrame head: %s', updates_df.head(5))
    logger.trace('Updates DataFrame tail: %s', updates_df.tail(5))
    
    updates_df = updates_df.rename(columns={0: axis1_name})
    updates_df['SObjectId'] = object_id
    update_suffix = '_update'
    axis1_name_updated = axis1_name + update_suffix
    # Merge the updates into the original DataFrame
    df = pd.merge(df, updates_df, on=['SObjectId', 'FieldName'], how='left', suffixes=('', update_suffix))

    # Use the updated values where they exist
    # df[axis1_name] = df[axis1_name_updated].where(df[axis1_name_updated].notna(), df[axis1_name])

    # Drop the update columns
    # df = df.drop(columns=[axis1_name_updated])

    df.to_csv('data_map.csv', index=False)
    return df


def make_request_and_edit_query_until_success(object_name, fields, soql_function):
    query_string = soql_function(object_name, fields)
    retry_limit = len(fields) - 1
    parsed_response = query.run_query_using_requests(query_string)
    if parsed_response.is_error() and not parsed_response.is_object_error():
        while parsed_response.is_error() and retry_limit > 0 and not parsed_response.is_object_error():
            retry_limit -= 1
            fields = query.removed_errored_fields(object_name, fields, parsed_response.export_dto()['errorMessage'], parsed_response.export_dto()['errorCode'])
            parsed_response = query.run_query_using_requests(soql_function(object_name, fields))
    if parsed_response.is_error():
        logger.error('Error occurred while querying Salesforce API: %s', parsed_response.export_dto()['errorMessage'])

    return parsed_response, fields

def query_counts_of_non_null_field_values(object_name, fields):
    logger.debug('Querying counts of non-null field values for object %s', object_name)
    fields = list(set(fields))
    soql_function = soql.get_count_of_fields_values
    return make_request_and_edit_query_until_success(object_name, fields, soql_function)

def calculate_counts_of_non_null_field_values(parsed_response, fields):
    counts_by_field_result = {}  # {field: count}
    if not parsed_response.is_error():
        record_counts = list(parsed_response.export_dto()['records'][0].values())[1:] if len(list(parsed_response.export_dto()['records'][0].values())) > 0 else 0
        if record_counts == 0 or sum(record_counts) == 0:
            for field in fields:
                counts_by_field_result[field] = 0
        else:
            counts_by_field_result = match_fields_to_counts(record_counts, fields)
    return counts_by_field_result

def match_fields_to_counts(record_counts, fields):
    logger.debug('Matching fields to counts')
    logger.debug('Record counts: %s', record_counts)
    logger.debug('Fields: %s', fields)
    output = {}
    for count in record_counts:
        output[fields[record_counts.index(count)]] = count
    logger.debug('Output: %s', output)
    return output

def query_most_common_values_in_field(object_name, field):
    return query.run_query_using_requests(soql.get_most_common_values(object_name, field))

def calculate_most_common_values_in_field(parsed_response, field):
    most_common_values_by_field_result = {field:[]}  # {field: [most common values]}
    dto = parsed_response.export_dto()
    for record in dto['records']:
        try:
            value = record[field]
            count = record['expr0']
            if value == None:
                continue
        except KeyError:
            raise ValueError('Field ' + field + ' not found in response')
        most_common_values_by_field_result[field].append([value, count])

    # Sorting the list from high to low
    most_common_values_by_field_result[field] = sorted(most_common_values_by_field_result[field], key=lambda x: x[1], reverse=True)

    return most_common_values_by_field_result
"""    for field in fields:
        most_common_values_in_field = query_most_common_values_in_field(object_name, field)
        if most_common_values_in_field['records']:
            most_common_values = set()
            for i in range(len(fields)):
                key = 'expr' + str(i)
                most_common_values.update(set([item[key] for item in most_common_values_in_field['records'] if key in item]))
            try:
                final_datamap = save_datamap(final_datamap, field, 'MostCommonValues', most_common_values)
            except ValueError as e:
                print(most_common_values)
"""
def query_most_recent_created_date_in_field(object_name, field):
    return query.run_query_using_requests(soql.get_last_created_with_field(object_name, field))

def calculate_most_recent_created_date_in_field(parsed_response):
    # if parsed_response['records'] has records, it should have a single record, because query was ORDER BY DESC and LIMIT 1.
    created_date = ''
    dto = parsed_response.export_dto()
    try:
        assert len(dto['records']) <= 1
    except AssertionError as e:
        logger.error('More than one record returned for query: ' + dto['queryString'])
    try:
       created_date = dto['records'][0]['CreatedDate']
       created_date = pd.to_datetime(created_date)
    except IndexError as e:
        pass

    return created_date


def main():
    try:
        # Salesforce to Salesforce means we don't need standard objects. We can just use the custom objects.
        # Get the object ids and their names.
        object_ids_to_object_names = query.match_custom_object_ids_to_object_names()
        # Get the object ids and their fields.
        object_to_fields = query.match_custom_object_ids_to_field_names()
        # Create a dataframe where the field names are unique, and the object ids are duplicated for each field (1:M).
        df_object_to_fields = pd.DataFrame(object_to_fields.items(), index=object_to_fields.keys(), columns=['SObjectId', 'FieldName']).explode('FieldName')
        # Create a dataframe where the object names are associated to their object ids. 
        df_object_ids_to_object_names = pd.DataFrame(object_ids_to_object_names.items(), index=object_ids_to_object_names.keys(), columns=['SObjectId', 'SObjectName'])
        # Merge the two dataframes together, so now we have object ids names and their fields.
        final_datamap = df_object_to_fields.merge(df_object_ids_to_object_names, on='SObjectId', how='left')

        for object_id in list(set(object_to_fields.keys())):  # For each object Id...
            # Get the fields associated to the object id.
            df_unique_fields = df_object_to_fields  #
            try:
                fields = df_unique_fields.loc[df_unique_fields.loc[object_id, 'SObjectId'], 'FieldName']
                if type(fields) == str:
                    fields = [fields]
                else:
                    fields = list(set(fields.tolist()))
            except (AttributeError, TypeError) as e:
                logger.error('No fields found for object id: ' + str(object_id))
                continue
            # Get the object name associated to the object id.
            try:
                object_name = df_object_ids_to_object_names.loc[df_object_ids_to_object_names.loc[object_id, 'SObjectId'], 'SObjectName']
            except KeyError as e:
                logger.error('No object name found for object id: ' + str(object_id))
                continue
            ## Counts of non-null field values
            # Get the record counts for all the fields associated to the object id. We can batch this aggregate, but the others need DataFrames.
            dto_counts_of_non_null_field_values, final_fields = query_counts_of_non_null_field_values(object_name, fields) # Returns a data transfer object
            if not dto_counts_of_non_null_field_values.is_error():
                # Send the Dto for processing count totals.
                counts_of_non_null_field_values = calculate_counts_of_non_null_field_values(dto_counts_of_non_null_field_values, final_fields)
                # Update the results to return.
                if counts_of_non_null_field_values:
                    final_datamap = save_datamap(final_datamap, object_id,  'CountOfNonNullValues', counts_of_non_null_field_values)
                    for field in fields:
                        ## Most common values per fields
                        most_common_values_in_field = query_most_common_values_in_field(object_name, field)
                        # Send the Dto for processing most common values.
                        if most_common_values_in_field.is_error():
                            continue
                        most_common_values_in_field = calculate_most_common_values_in_field(most_common_values_in_field, field)
                        ## Most recent created date per field
                        most_recent_created_date_in_field = query_most_recent_created_date_in_field(object_name, field) # Returns a data transfer object
                        most_recent_created_date_in_field = calculate_most_recent_created_date_in_field(most_recent_created_date_in_field)
                        # Update the results to return.
                        if most_common_values_in_field:
                            final_datamap = save_datamap(final_datamap, object_id, 'MostCommonValues', most_common_values_in_field)
                        if most_recent_created_date_in_field:
                            final_datamap = save_datamap(final_datamap, object_id, 'DateOfLastValue', most_recent_created_date_in_field)

    except AttributeError as e:
        print(e)

if __name__ == "__main__":
    main()

