import os
import boto3
import csv

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def get_dynamo_db_table_name():
    """
    This function gets table name of the DynamoDB.
    In the YAML template, we define an Environment in Lambda Function that gets
    the CustomerDDBTable as TABLE. We can get the value of TABLE by using os.environ['TABLE']

    Parameters:

    None

    Returns:

    Table name. Otherwise, None
    
    """    
    ret = None
    try:
        table_name = os.environ['TABLE']
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        ret = table_name
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')
        return ret
    
def parse_csv_ddb(csv_filename):
    """
    This function parses .csv file and returns its contents as a dictionary.

    Parameters:

    csv_filename: The .csv filename to parse.

    Returns:
    
    Contents of the .csv file as a dictionary. Otherwise, None.

    """    
    details_reader = None
    try:
        with open(csv_filename, newline='') as f:
            reader = csv.DictReader(f)
            details_reader = next(reader) # return a dictionary
                        
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

        return details_reader
    
def update_ddb_with_customer_info(details_file, appuuid, customer_details, ddb_response, valerror):
    """
    This function adds customer's personal details (in .csv file) to DynamoDB table

    Parameters:

    details_file: Customer's personal details (.csv file)
    appuuid: Customer's ID which is used as DynamoDB partition key
    customer_details: Returned dictionary that contains DynamoDB table name and Customer's detailed info.
    ddb_response: Returned response from DynamoDB.
    valerror: returned exception error

    Returns:

    True if operations are successful. Otherwise, False
    
    """    

    ret = False

    try:

        ddb_table_name = get_dynamo_db_table_name()
        if not ddb_table_name:
            raise ValueError('No DynamoDB table')
        print(f'ddb_table_name: {ddb_table_name}')
        
        # From Boto3 documentation:
        # Instantiate a table resource object without actually creating a DynamoDB table.
        ddb_table = dynamodb.Table(ddb_table_name)
        if ddb_table is None:
            raise ValueError('Table: test_table not found')
        # For unit testing, the following if-condition will not work unless I have IAM Policy "DerscribeTable".
        # The Lab gives me AccessDenied when I want to add this IAM Policy.
        # A temporary solution is to use the above if-condition and then mock dynamodb.Table in unit testing.
        #if 'not found' in ddb_table.table_status:
            # Boto3 documenation: the attributes (such as table_status) are lazy-loaded,
            # which means that these are not fetched immediately when the object is created.
            # So, once I call ddb_table.table_status, its value will be fetched.
            # If its value has an error exception, then the exception automatically occurs
            # before even executing the following line of code:
        #    raise ValueError()
        print(f'ddb_table: {ddb_table}')
        
        # Parse csv file and get a dictionary
        details_dic = parse_csv_ddb(details_file)
        if not details_dic:
            raise ValueError('Could not parse csv file')
        print(f'details_dic: {details_dic}')
        
        # Write the dictionary to DynamoDB table.
        # Item: attributes for the primary key (partition key).
        # For the partition key, see KeySchema in YAML. It is a Hash Key for appuuid.
        # So, for one item (details_dic), there is one unique partition key.
        # For Valid DynamoDB Types, see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#ref-valid-dynamodb-types
        #  It shows: S for string, M for dictionary, N for integer, L for list, etc.
        ddb_response['ddb_response'] = ddb_table.put_item(Item={**details_dic, "APP_UUID": appuuid}) # see APP_UUID in AttributeName in YAML DynamoDB
        if ddb_response['ddb_response']['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError('Could not put DynamoDB Table item')

    except (Exception, ValueError) as error:
        print(f'Exception error: update_ddb_with_customer_info : {error}')
        valerror['error'] = error

    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: update_ddb_with_customer_info :')
        
        customer_details['ddb_table'] = ddb_table
        customer_details['details_dic'] = details_dic

        ret = True

    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: update_ddb_with_customer_info :')

    return ret

def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for WriteToDynamoLambdaFunction.

    Parameters:

    event: UnzipLambdaFunction event, which contains bucket name and app_uuid
    context: not used in this application

    Returns:

    A dictionary that contains driver_license_id, validation_override, and appuuid. Otherwise, None.
    
    """    
    
    print(f'Entering lambda handler for WriteToDynamoLambdaFunction')
    
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    LAMBDA_TMP_FOLDER = '/tmp/'
    LAMBDA_UNZIPPED_FOLDER = 'unzipped/'
    
    ret = None

    try:
        detail = event['detail']
        bucket = detail['bucket']['name']
        application = event['application']
        appuuid = application['app_uuid']

        print(f'detail: {detail}')
        print(f'bucket: {bucket}') # e.g. bucket: documentbucket-115476135777
        print(f'application: {application}')
        print(f'app_uuid: {appuuid}')  # e.g. app_uuid: 8d247914

        # Create a subfolder in Lambda's tmp folder
        subfolder_path = LAMBDA_TMP_FOLDER + LAMBDA_UNZIPPED_FOLDER
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)

        # Download the .csv file from S3 bucket to this Lambda's internal memory
        # Use: s3.download_file(bucket, from, to)
        location_in_bucket = BUCKET_UNZIPPED_PREFIX + appuuid + '_details.csv'
        details_file = LAMBDA_TMP_FOLDER + LAMBDA_UNZIPPED_FOLDER + appuuid + '_details.csv'
        print(f'location_in_bucket: {location_in_bucket}')
        print(f'details_file: {details_file}')
        response_s3 = s3.download_file(bucket, location_in_bucket, details_file)

        #==============================================================
        # Put customer's personal details (.csv file) in DynamoDB table
        #==============================================================
        customer_details = {'ddb_table':'', 'details_dic':{}}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}
        outcome = update_ddb_with_customer_info(details_file, appuuid, customer_details, ddb_response, valerror)
        if outcome == False:
            raise ValueError('Error in update_ddb_with_customer_info')
        
        ddb_table = customer_details['ddb_table']
        details_dic = customer_details['details_dic']

        response = {'driver_license_id': details_dic.get('DOCUMENT_NUMBER', '0'), # if 'DOCUMENT_NUMBER' does not exist, it returns '0'
                    'validation_override': True,
                    'app_uuid': appuuid}
        
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        
        ret = response
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

        return ret

