import os
import boto3
import csv

CUSTOMER_INFORMATION = [
    'DOCUMENT_NUMBER',
    'FIRST_NAME',
    'LAST_NAME',
    'DATE_OF_BIRTH',
    'ADDRESS',
    'STATE_IN_ADDRESS',
    'CITY_IN_ADDRESS',
    'ZIP_CODE_IN_ADDRESS']

SNS_IDMATCH_MESSAGE = 'No matches between Customer ID and Submitted Customer Info'
SNS_IDMATCH_SUBJECT = 'Customer ID Info Match Fails'

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
textract = boto3.client('textract')
   
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
    
def analyze_document_id(
        bucket_name,
        document_id):
    """
    This function analyzes a document using AWS Textract service and returns extracted fields from the document.

    Parameters:

    bucket_name: Name of s3 bucket where the document filename is stored.
    document_id: Name of the document filename in S3 bucket.

    Returns:
    
    response: A dictionary. See analyze_id() boto3 documentation.

    """    
    ret = None
    try:
        response = textract.analyze_id(
            DocumentPages=[
                {
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': document_id
                    }
                }
            ]
        )
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

def get_customer_extracted_info(response):
    """
    This function returns the extracted information from the analyze_id() AWS Texract function
    as a dictionary.

    Parameters:

    response: A dictionary response from the analyze_id() function.
 
    Returns:

    Extracted information as a dictionary. Otherwise, None
    
    """    
    ret = None
    extracted_info = {}
    try:
        # IdentityDocuments: An array (in the dictionary) of documents that were analyzed by analyze_id()
        if len(response['IdentityDocuments']) == 0:
            # The array is empty. Hence, no document was processed by analyze_id()
            return None 
        
        # There is at least one document being analyzed.
        document_dict = response['IdentityDocuments'][0]
        document_fields = document_dict['IdentityDocumentFields'] # This is a list of dictionaries

        for field in document_fields:
            if field['Type']['Text'] in CUSTOMER_INFORMATION:
                extracted_info[field['Type']['Text']] = field['ValueDetection']['Text']

    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        ret = extracted_info
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')
        return ret

def is_matching_customer_info(
        extracted_info,
        customer_info):
    """
    This function compares the extracted customer information from the document that was analyzed by
    the analyze_id() with the corresponding customer information submitted by the customer.

    Parameters:

    extracted_info: Extracted customer information from the document that was analyzed.
    customer_info: Customer information submitted by the customer

    Returns:
    
    True if the response from the analyze_id() matches corresponding customer information
    submitted by the customer. Otherwise, False

    """    
    ret = False
    
    try:
        if extracted_info == customer_info:
            ret = True
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')
        return ret
    
def get_sns_topic_name():
    """
    This function gets SNS Topic name.
    In the YAML template, we define an Environment in Lambda Function that gets
    the ApplicationNotifications as TOPIC. We can get the value of TOPIC by using os.environ['TOPIC']

    Parameters:

    None

    Returns:

    SNS Topic name. Otherwise, None
    
    """    
    ret = None
    try:
        sns_topic_name = os.environ['TOPIC']
    except Exception as error:
        print(f'Exception error: get_sns_topic_name : {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: get_sns_topic_name :')
        ret = sns_topic_name
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: get_sns_topic_name :')
        return ret
    
def send_sns_email(message, subject):
    """
    This function sends SNS Email

    Parameters:

    message: Email message
    subject: Email subject

    Returns:

    True if SNS is sent. Otherwise, False.
    
    """    
    ret = False
    try:
        topic_name = get_sns_topic_name()
        if topic_name is None:
            raise ValueError('Could not get SNS Topic!')
                
        response_sns = sns.publish(
            TopicArn = topic_name,
            Message = message,
            Subject = subject)
            
        if response_sns is None:
            raise ValueError('Could not publish SNS!')
                
        print(f'Sent SNS to Topic Name: {topic_name}')

    except Exception as error:
        print(f'Exception error: send_sns_email : {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: send_sns_email :')
        ret = True
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: send_sns_email :')
        return ret

def validate_customer_details(bucket, license_key, appuuid, ddb_table, details_dic):
    """
    This function compares customer's submitted info (in details_dic) with
    customer's driver license (in license_key) using AWS Textract,
    updates DynamoDB table (LICENSE_DETAILS_MATCH attribute) with the outcome of this comparison,
    and sends an email if the comparison fails.

    Parameters:

    bucket: S3 bucket name where the customer's driver license image (license_key image) is stored.
    license_key: Customer's driver license image
    appuuid: Customer's ID, which is also the partition key for DynamoDB table
    ddb_table: DynamoDB table name
    details_dic: Customer's submitted info (from .csv file)

    Returns:

    True if operations are successful. Otherwise, False
    
    """    
    
    ret = False

    try:
        # Analyze customer's submitted document ID.
        response_textract = analyze_document_id(bucket, license_key)
        if response_textract is None:
            raise ValueError('Could not analyze customer\'s ID')
        print(f'Analysis of customer submitted ID: {response_textract}')
        
        # Extract customer's information from the submitted ID.
        extracted_info = get_customer_extracted_info(response_textract)
        if extracted_info is None:
            raise ValueError('Could not extract customer\'s information from the ID')
        print(f'Extracted info from customer submitted ID: {extracted_info}')
        
        # Compare extracted information with customer's submitted information
        matches_info_found = is_matching_customer_info(extracted_info, details_dic)
        print(f'Is matching customer info?: {matches_info_found}')
        
        # Update LICENSE_DETAILS_MATCH attribute according to matches_info_found result (i.e True/False). 
        #  The DynamoDB update_item() will create the attribute if it does not exist.
        response_db_update = ddb_table.update_item(
            Key={"APP_UUID": appuuid},
            UpdateExpression='SET LICENSE_DETAILS_MATCH=:f_matches',
            ExpressionAttributeValues={':f_matches':matches_info_found})
        if response_db_update is None:
            raise ValueError('Could not update DynamoDB Table item with LICENSE_DETAILS_MATCH')
        print(f'Response to update LICENSE_DETAILS_MATCH attribute: {response_db_update}')
        
        # Send SNS email if a match is not found, and then raise an exception
        if matches_info_found is False:
            # Send SNS
            send_sns_email(SNS_IDMATCH_MESSAGE, SNS_IDMATCH_SUBJECT)
            raise ValueError('Could not match Customer ID with submitted Customer info')
        
        print(f'No SNS is being sent')

    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        
        ret = True
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

    return ret


def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for CompareDetailsLambdaFunction.

    Parameters:

    event: State event, which contains app_uuid and bucket name
    context: not used in this application

    Returns:

    A dictionary that contains a status as either failure or success,
    with a message describing the status.
    
    """    
    
    print(f'Entering lambda handler for CompareDetailsLambdaFunction')
    
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    LAMBDA_TMP_FOLDER = '/tmp/'
    LAMBDA_UNZIPPED_FOLDER = 'unzipped/'
    
    ret = {
        "status": "failure",
        "message": "ID Information Comparison failed"
    }

    try:
        print(f'event: {event}')
        
        detail = event['detail']
        bucket = detail['bucket']['name']
        application = event['application']
        appuuid = application['app_uuid']

        print(f'detail: {detail}')
        print(f'bucket: {bucket}') # e.g. bucket: documentbucket-115476135777
        print(f'application: {application}')
        print(f'appuuid: {appuuid}')
                
        # Create a subfolder in Lambda's tmp folder
        subfolder_path = LAMBDA_TMP_FOLDER + LAMBDA_UNZIPPED_FOLDER
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)

        license_key = BUCKET_UNZIPPED_PREFIX + appuuid + '_license.png'

        # Download the .csv file from S3 bucket to this Lambda's internal memory
        # Use: s3.download_file(bucket, from, to)
        location_in_bucket = BUCKET_UNZIPPED_PREFIX + appuuid + '_details.csv'
        details_file = LAMBDA_TMP_FOLDER + LAMBDA_UNZIPPED_FOLDER + appuuid + '_details.csv'
        print(f'location_in_bucket: {location_in_bucket}')
        print(f'details_file: {details_file}')
        response_s3 = s3.download_file(bucket, location_in_bucket, details_file)

        # Parse .csv file and get a dictionary
        details_dic = parse_csv_ddb(details_file)
        if details_dic is None:
            raise ValueError('Could not parse csv file')
        print(f'details_dic: {details_dic}')

        # Get DynamoDB table. The status of Textract comparison operation will be written in the table.
        ddb_table_name = get_dynamo_db_table_name()
        if ddb_table_name is None:
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

        #=====================================================================================================
        # Compare customer's submitted info (in details_dic) with customer's driver license using AWS Textract.
        # Update DynamoDB table the outcome of this comparison.
        # Send an email if the comparison fails.
        #=====================================================================================================
        outcome = validate_customer_details(bucket, license_key, appuuid, ddb_table, details_dic)
        if outcome == False:
            raise ValueError('Error in validate_customer_details')
                        
    except Exception as error:
        print(f'Exception error: {error}')
        
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        
        ret = {
            "status": "success",
            "message": "ID Information Comparison successful"
        }
    
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

        return ret

