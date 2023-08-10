import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import salesforce.query as query
import salesforce.soql as soql
import salesforce.log_config as log_config

# Initialize logging.
logger = log_config.get_logger(__name__)
LIMIT = 3  # Like SQL LIMIT for records returned to logger.

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

def create_dataframe_from_common_values(field_name, object_id, values_and_counts, axis1_name='MostCommonValues'):
    """
    Creates a DataFrame from a dictionary of field and common values.

    Args:
    - field_name (str): Name of the field being processed.
    - object_id (str): Id of the object being processed.
    - values_and_counts (dict): Dictionary containing field name as key and list of [value, count] pairs.
    - axis1_name (str, optional): Base name for DataFrame columns. Defaults to 'MostCommonValues'.

    Returns:
    - DataFrame: DataFrame populated with most common values and their counts.
    """
    logger.debug('Creating DataFrame from common values for field: %s', field_name)
    logger.debug('Fields and values: %s', values_and_counts)

    columns = ['SObjectId', 'FieldName']  # These are the two main columns across this component.
    for i in range(1, 4):  # We only need to count to 3, but we need a better solution for this.
        columns.append(axis1_name + str(i))
        columns.append(axis1_name + str(i) + 'Count')

    df = pd.DataFrame(columns=columns)  # Create the DataFrame with the new columns.
    if field_name in values_and_counts:
        for index, (value, count) in enumerate(values_and_counts[field_name], start=1):
            df[axis1_name + str(index)] = [value]
            df[axis1_name + str(index) + 'Count'] = [count]
            if index == 3:  # We only need top 3
                break
            elif index == 2:
                logger.debug('Field: %s, Value: %s, Count: %s', field_name, value, count)
            else:
                pass
        if not df.empty:
            df['SObjectId'] = object_id
            df['FieldName'] = field_name
    else:
        logger.debug(f'Empty input?? {field_name} not in {values_and_counts}')
        assert list(values_and_counts.values()) == 0
        assert list(values_and_counts.keys()) == 0

    logger.debug('Updates DataFrame head: %s', df.head(LIMIT))

    return df

def create_dataframe_from_recent_date(object_id, field_name, value, axis1_name='DateOfLastValue'):
    # Log
    logger.debug('Creating DataFrame for field: %s', field_name)
    logger.debug('Value is: %s', value)
    # Turn the updates into a DataFrame
    updates_df = pd.DataFrame.from_records({field_name: value}).transpose().reset_index().rename(columns={'index': 'FieldName'})
    updates_df['SObjectId'] = object_id
    # Log
    logger.debug('Updates DataFrame head: %s', updates_df.head(LIMIT))
    updates_df = updates_df.rename(columns={0: axis1_name})
    return updates_df

def create_dataframe_from_value_counts(object_id, fields_and_values, axis1_name='CountOfNonNullValues'):
    # Log
    logger.debug(f'Adding field {object_id}.{axis1_name}: with count {fields_and_values}')
    # Turn the updates into a DataFrame
    updates_df = pd.DataFrame.from_records([fields_and_values]).transpose().reset_index().rename(columns={'index': 'FieldName'})
    updates_df['SObjectId'] = object_id
    # Log
    logger.debug('Updates DataFrame head: %s', updates_df.head(LIMIT))
    updates_df = updates_df.rename(columns={0: axis1_name})
    return updates_df

def save_datamap(df, save_index=False):
    if df.shape[1] > 9:
        logger.debug('Trying to reproduce bug')
    df.to_csv('data_map9A.csv', index=save_index)
    return df


def merge_dataframes_for_field_mapping(df, to_merge, object_id):
    og = df.copy()  # Stored for debugging
    logger.debug('Original df head: %s', og.loc[object_id])
    update_suffix = '_update'
    # Merge the updates into the original DataFrame
    df['SObjectId'] = df['SObjectId'].astype(str)
    df['FieldName'] = df['FieldName'].astype(str)
    df.set_index(['FieldName', 'SObjectId'], inplace=True)
    to_merge['SObjectId'] = to_merge['SObjectId'].astype(str)
    to_merge['FieldName'] = to_merge['FieldName'].astype(str)
    to_merge.set_index(['FieldName', 'SObjectId'], inplace=True)
    for col in to_merge.columns.difference(df.columns):  # Add any columns that don't exist in the original DataFrame
        logger.info(f'Adding column {col} to original DataFrame')
        df[col] = None
    try:
        logger.debug('Testing merge')
        logger.debug(f'Main df head\n {to_merge.head(LIMIT)}')
        logger.debug('TEST - .loc() %s', df.loc[df.loc[:, list(to_merge.columns.values)], list(to_merge.columns.values)].head(LIMIT))
        loc_df = df.copy()
        loc_df.loc[loc_df.loc[:, loc_df.loc[(list(to_merge.columns.values))], list(to_merge.columns.values)]] = to_merge
        logger.debug('After .loc() %s', loc_df.loc[loc_df.loc[:, list(to_merge.columns.values)], list(to_merge.columns.values)])
        logger.debug('TEST - .merge() %s', pd.merge(df, to_merge, left_index=True, right_index=True, how='left'))
        left_outer_join = pd.merge(df, to_merge, left_index=True, right_index=True, how='left')
        logger.debug('After .merge() %s',  left_outer_join.loc[left_outer_join.loc[:, list(to_merge.columns.values)], list(to_merge.columns.values)].head(LIMIT))
        logger.debug('TEST - .join() %s', df.join(to_merge, how='left', lsuffix='', rsuffix='_update'))
        df.update(to_merge)
        logger.debug('After .join() %s', df.loc[df.loc[:, list(to_merge.columns.values)], list(to_merge.columns.values)].head(LIMIT))
        logger.info('Merged updates into original DataFrame for object: %s', object_id)
        #df = pd.merge(df, to_merge, on=['SObjectId', 'FieldName'], how='left', suffixes=('', update_suffix))
        #try:
        #    df[column] = df[column + update_suffix].where(df[column + update_suffix].notna(), df[column])
        #    logger.info('Merged updates into original DataFrame for field: %s', column)
        #except KeyError as e:
        #    logger.debug(f'Nothing was merged, so column does not exist. {e}')
    except ValueError as e:
        logger.debug(f'Nothing was merged, so column does not exist. {e}')
    except pd.errors.MergeError as e:
        logger.error(f'Error merging updates into original DataFrame, {e}')
        logger.warning('Skipping this field. Final data map won\'t include it.')

    cols_to_drop = df.filter(regex='_update$').columns
    df = df.drop(columns=cols_to_drop)
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

def query_count_of_non_null_field_values(object_name, field):
    """
    Queries the count of non-null field values for a given object.
    `field` is singular. If you want to query multiple fields, use `query_counts_of_non_null_field_values`.
    
    :param object_name: Name of the object to query.
    :param field: Field or list of fields to check for non-null values.
    :return: Count of non-null field values.
    """
    
    # Logging the operation for debugging purposes.
    logger.debug('Querying count of non-null field values for object %s', object_name)
    
    return query.run_query_using_requests(soql.get_count_of_field_values(object_name, field))

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
    if parsed_response.get_record_count() > 0:
        dto = parsed_response.export_dto()
        try:
            created_date = dto['records'][0]['CreatedDate']
            created_date = pd.to_datetime(created_date)
        except IndexError as e:
            logger.info('No records returned for query: ' + dto['queryString'])
        if created_date == '':
            logger.info('No records returned for query: ' + dto['queryString'])

    return created_date


def main(save_test_fixtures=False):
    """

    :param save_test_fixtures:
    :return:
    """
    if save_test_fixtures:
        from tests.save_dataframes_as_fixtures_for_testing import save_test_fixture
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
        # Set the finalmap index to SObject and Fieldname
        final_datamap.set_index(['SObjectId', 'FieldName'], inplace=True)
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
            for field in fields:
                ## Counts of non-null field values
                # Get the record counts for all the fields associated to the object id. We can batch this aggregate, but the others need DataFrames.
                dto_counts_of_non_null_field_values, final_fields = query_counts_of_non_null_field_values(object_name, [field])  # Returns a data transfer object
                if not dto_counts_of_non_null_field_values.is_error() and dto_counts_of_non_null_field_values.get_record_count() > 0:
                    # Send the Dto for processing count totals.
                    counts_of_non_null_field_values = calculate_counts_of_non_null_field_values(dto_counts_of_non_null_field_values, final_fields)
                    # Update the results to return.
                    if counts_of_non_null_field_values:
                        df_count_values = create_dataframe_from_value_counts(object_id, counts_of_non_null_field_values)
                        if save_test_fixtures:
                            save_test_fixture(df_count_values, 'counts_of_non_null_field_values')
                        final_datamap = merge_dataframes_for_field_mapping(final_datamap, df_count_values, object_id)
                        final_datamap = save_datamap(final_datamap)

                ## Most common values per fields
                most_common_values_in_field = query_most_common_values_in_field(object_name, field)
                # Send the Dto for processing most common values.
                if not most_common_values_in_field.is_error() and most_common_values_in_field.get_record_count() > 0:
                    most_common_values_in_field = calculate_most_common_values_in_field(most_common_values_in_field, field)
                    df_common_values = create_dataframe_from_common_values(field, object_id, most_common_values_in_field)
                    if save_test_fixtures:
                        save_test_fixture(df_common_values, 'most_common_field_values')
                    final_datamap = merge_dataframes_for_field_mapping(final_datamap, df_common_values, object_id)
                    final_datamap = save_datamap(final_datamap)
                else:
                    logger.info('No common values found for field: %s', field)
                ## Most recent created date per field
                most_recent_created_date_in_field = query_most_recent_created_date_in_field(object_name, field) # Returns a data transfer object
                # Update the results to return.
                if not most_recent_created_date_in_field.is_error() and most_recent_created_date_in_field.get_record_count() > 0:
                    most_recent_created_date_in_field = calculate_most_recent_created_date_in_field(most_recent_created_date_in_field)
                    df_recent_date = create_dataframe_from_recent_date(object_id, field, [most_recent_created_date_in_field])
                    if save_test_fixtures:
                        save_test_fixture(df_recent_date, 'most_recent_created_date')
                    final_datamap = merge_dataframes_for_field_mapping(final_datamap, df_recent_date, object_id)
                    final_datamap = save_datamap(final_datamap)

    except AttributeError as e:
        print(e)

if __name__ == "__main__":
    main()

