import os
import botocore
import boto3
import zipfile
import csv
import json

SIMILARITY_THRESHOLD = 80
CUSTOMER_INFORMATION = [
    'DOCUMENT_NUMBER',
    'FIRST_NAME',
    'LAST_NAME',
    'DATE_OF_BIRTH',
    'ADDRESS',
    'STATE_IN_ADDRESS',
    'CITY_IN_ADDRESS',
    'ZIP_CODE_IN_ADDRESS']
SNS_FACEMATCH_MESSAGE = 'No matches between selfie and license'
SNS_FACEMATCH_SUBJECT = 'Face Match Fails'
SNS_IDMATCH_MESSAGE = 'No matches between Customer ID and Submitted Customer Info'
SNS_IDMATCH_SUBJECT = 'Customer ID Info Match Fails'

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
sns = boto3.client('sns')
textract = boto3.client('textract')
sqs = boto3.client('sqs')

def unzip_file(zipfile_filename, path_of_unzipped_file = None):
    """
    This function unzip a given file.

    Parameters:

    zipfile_filename: The filename of the Zipped File to unzip
    path_of_unzipped_file: The path where the Zipped File is unzipped.
                           If not provided, then this function will unzip to the same path of Zipped File.
                           If provided, but it does not exist, then extractall() will create it.

    Returns:

    True if the unzip is successful. Otherwise, False

    """
    
    ret = False

    try:

        if zipfile_filename is None:
            raise ValueError('File Name is None')
        
        with zipfile.ZipFile(zipfile_filename, mode='r') as zipped_file_object:
            zipped_file_object.extractall(path=path_of_unzipped_file)

    except FileNotFoundError as error:
        print(f'Exception error: {error}')
    except PermissionError as error:
        print(f'Exception error: {error}')
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

def get_unzipped_files(path_of_unzipped_file = None):
    """
    This function returns a list of files in a specified path

    Parameters:

    path_of_unzipped_file: The specified path.
                           If the specified path is None, it looks in the path where the executable is running

    Returns:

    A list of existing files. Otherwise, None
    
    """

    ret = None

    try:
        list_of_file = os.listdir(path_of_unzipped_file)
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        ret = list_of_file
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

        return ret

def upload_file_to_s3(
        s3,
        file_to_upload, 
        path_of_file, 
        bucket_name,
        file_key_name,
        prefix = ''):
    """
    This function uploads a file to S3.

    Parameters:

    s3: Boto3 S3 client
    file_to_upload: The file that you want to upload.
    path_of_file: The path where the file_to_upload is located.
    bucket_name: S3 Bucket Name where the file will be uploaded to.
    file_key_name: The S3 Key of the file_to_upload. Usually, this is the same as file name
    prefix: The prefix (or folder name) in the bucket where the file will be uploaded to.

    Returns:

    True if the upload is successful. Otherwise, False
    
    """    
    ret = False

    try:
    
        # Upload a new file
        # Per Lab3 instructions, the 'key' should include the 'prefix' (see Task 4). Thus, I am passing 'prefix + file_key_name'
        file_name_with_path = path_of_file + file_to_upload
        response = s3.upload_file(
            file_name_with_path,
            bucket_name,
            prefix + file_key_name)

        # response was None
        print(f'Response after uploading file to S3: {response}')

        # Then I ran the following command to verify the file is in the bucket:
        # aws s3 ls documentbucket-875269704541
        
    except botocore.exceptions.ClientError as error:
        print(f'Exception Client Error: {error}')

    except botocore.exceptions.ParamValidationError as error:
        print(f'Exception Value Error: {error}')

    # This is a for non-botocore exceptions
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

def get_app_uuid(file_name_with_extension):
    """
    This function gets the app_uuid from the file name, which has extension.
    For example, and per Lab3, if the file name is 123456.zip, then the app_uuid is 123456

    Parameters:

    file_name_with_extension: The file name with extension

    Returns:

    app_uuid. Otherwise, None
    
    """    
    ret = None
    try:
        appuuid = str.split(file_name_with_extension, '.')
        ret = appuuid[0]
    except Exception as error:
        print(f'Exception error: {error}')
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')
        return ret
    
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
    
def get_matching_faces(
        bucket_name,
        source_image,
        target_image,
        similarity_threshold,
        valerror):
    """
    This function compares two images using AWS Rekognition's compare_faces() function,
    and returns matching faces.

    Parameters:

    bucket_name: Name of s3 bucket where the two images are stored.
    source_image: Name of the source image filename in S3 bucket.
    target_image: Name of the target image filename in S3 bucket.
    similarity_threshold: The SimilarityThreshold used by compare_faces() function.
    valerror: returned exception error

    Returns:
    
    A dictionary of matching faces. See compare_faces() boto3 documentation.

    """    
    
    ret = None
    try:
        # Using the global rekognition client
        response = rekognition.compare_faces(
            SourceImage={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': source_image
                    }
                },
            TargetImage={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': target_image
                    }
                },
            SimilarityThreshold=similarity_threshold,
            QualityFilter='AUTO'
        )        
    except Exception as error:
        print(f'Exception error: get_matching_faces : {error}')
        valerror['error'] = error

    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: get_matching_faces :')
        ret = response

    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: get_matching_faces :')

        return ret

def is_matching_faces(response):
    """
    This function checks if the response from the AWS Rekognition's compare_faces() function contains matched faces.

    Parameters:

    response: A dictionary of matching faces from the compare_faces() function.

    Returns:

    True if there is a match. Otherwise, False
    
    """    
    ret = False
    # FaceMatches: An array (in the dictionary) of faces in the target image that match the source image face
    if len(response['FaceMatches']) == 0:
        # The array is empty. Hence, no matches
        ret = False 
    elif response['FaceMatches'][0]['Similarity'] < SIMILARITY_THRESHOLD:
        ret = False
    else:
        ret = True

    return ret

def analyze_document_id(
        bucket_name,
        document_id):
    """
    This function analyzes a document using AWS Textract service and returns extracted fields from the document.

    Parameters:

    bucket_name: Name of s3 bucket where the document filename is stored.
    document_id: Name of the document image filename in S3 bucket.

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

def send_sqs_message(message):
    """
    This function sends a message to AWS SQS.

    Parameters:

    message: The message to send to AWS SQS

    Returns:

    Response from SQS. Otherwise, None.
    
    """    
    ret = None
    try:
        sqs_name = os.environ['QUEUE_URL']

        response = sqs.send_message(
            QueueUrl=sqs_name,
            MessageBody=json.dumps(message)
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

def prepare_customer_info(bucket,
                          key,
                          lambda_tmp_folder,
                          lambda_unzipped_folder,
                          bucket_unzipped_prefix,
                          customer_info,
                          valerror):
    """
    This function gets .zip file from S3 bucket, unzip the file, then stores the unzipped objects in S3

    Parameters:

    bucket: S3 bucket name
    key: Zip filename prefixed with S3 folder name
    lambda_tmp_folder: This is the temporary folder of AWS Lambda. It is usually /tmp
    lambda_unzipped_folder: This is a subfolder in AWS Lambda's /tmp folder
    bucket_unzipped_prefix: S3 folder where unzipped files will be stored
    customer_info: returned dictionary that contains customer info
    valerror: returned exception error

    Returns:
    
    True if operations are successful. Otherwise, False

    """    

    ret = False

    try:

        # Get the zip filename
        zip_name = os.path.basename(key)
        zip_name_with_path = lambda_tmp_folder + zip_name
        print(f'Name of zip file with full path is {zip_name} and {zip_name_with_path}')
            
        # Download the .zip file from zipped/ prefix in S3 Bucket.
        # Store the downloaded file in the lambda folder 'tmp/'
        s3.download_file(
            bucket,
            key,
            zip_name_with_path)

        # Unzip the downloaded file to 'tmp/unzipped'
        print('Ready to unzip the file...')
        ret_unzip = unzip_file(zip_name_with_path, lambda_tmp_folder + lambda_unzipped_folder)
        if ret_unzip == False:
            raise ValueError('Error while unzipping a file')

        # Get a list of files in the unzipped folder
        list_of_files = get_unzipped_files(lambda_tmp_folder + lambda_unzipped_folder)
        if list_of_files is None:
            raise ValueError('No files to upload to S3')
        print(f'list_of_files: {list_of_files}')

        # Upload each file to S3 Bucket in unzipped/ prefix
        for file in list_of_files:
            ret_upload = upload_file_to_s3(
                s3,
                file, 
                lambda_tmp_folder + lambda_unzipped_folder, 
                bucket, 
                file, 
                bucket_unzipped_prefix)
            if ret_upload == False:
                raise ValueError('Error in uploading a file to S3')
                
        appuuid = get_app_uuid(zip_name)
        print(f'app uuid: {appuuid}')
        
        selfie_key = bucket_unzipped_prefix + appuuid + '_selfie.png'
        license_key = bucket_unzipped_prefix + appuuid + '_license.png'
        details_file = lambda_tmp_folder + lambda_unzipped_folder + appuuid + '_details.csv'

        customer_info['selfie_key'] = selfie_key
        customer_info['license_key'] = license_key
        customer_info['details_file'] = details_file
        customer_info['appuuid'] = appuuid

        print(f'selfie_key: {selfie_key}')
        print(f'license_key: {license_key}')
        print(f'details_file: {details_file}')
    
    except Exception as error:
        print(f'Exception error: prepare_customer_info : {error}')
        valerror['error'] = error
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: prepare_customer_info :')
        ret = True
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: prepare_customer_info :')

    return ret
    
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

def validate_selfie(bucket, selfie_key, license_key, appuuid, ddb_table, valerror):
    """
    This function compares two images (selfie_key and license_key) using AWS Rekognition,
    updates DynamoDB table (LICENSE_SELFIE_MATCH attribute) with the outcome of this comparison,
    and sends an email if the comparison fails.

    Parameters:

    bucket: S3 bucket name where the two images are stored.
    selfie_key: The first image (Customer's selfie image)
    license_key: The second image (Customer's driver license)
    appuuid: Customer's ID, which is also the partition key for DynamoDB table
    ddb_table: DynamoDB table name
    valerror: returned exception error

    Returns:

    True if operations are successful. Otherwise, False
    
    """    
    
    ret = False

    try:
    
        # Compare selfie_key with license_key using AWS Rekognition, and get an array of matching faces.
        matching_faces = get_matching_faces(
            bucket,
            selfie_key,
            license_key,
            SIMILARITY_THRESHOLD,
            valerror)
        if matching_faces['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError('Could not compare images')
        print(f'Possible Matching Faces: {matching_faces}')
        
        # Check if matches found
        matches_found = is_matching_faces(matching_faces)
        print(f'Is matches found?: {matches_found}')

        # Update LICENSE_SELFIE_MATCH attribute according to match-found result (i.e True/False). 
        #  The DynamoDB update_item() will create the attribute if it does not exist.
        response_db_update = ddb_table.update_item(
            Key={"APP_UUID": appuuid},
            UpdateExpression='SET LICENSE_SELFIE_MATCH=:f_matches',
            ExpressionAttributeValues={':f_matches':matches_found})
        if response_db_update['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ValueError('Could not update DynamoDB Table item with LICENSE_SELFIE_MATCH')
        print(f'Response to update LICENSE_SELFIE_MATCH attribute: {response_db_update}')
        
        # Send SNS email if a match is not found, and then raise an exception
        if matches_found is False:
            # Send SNS
            send_sns_email(SNS_FACEMATCH_MESSAGE, SNS_FACEMATCH_SUBJECT)
            raise ValueError('Could not match selfie with license')
        
        print(f'No SNS is being sent')

    except Exception as error:
        print(f'Exception error: validate_selfie : {error}')
        valerror['error'] = error

    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: validate_selfie :')
        
        ret = True

    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: validate_selfie :')

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

def queue_customer_id(appuuid, details_dic):
    """
    This function writes customer's driver license ID to Amazon SQS queue.

    Parameters:

    appuuid: Customer's unique ID
    details_dic: Customer's detailed info (which includes driver license ID)

    Returns:

    True if operations are successful. Otherwise, False.
    
    """    
    
    ret = False

    try:

        message = {'driver_license_id': details_dic.get('DOCUMENT_NUMBER', '0'), # if 'DOCUMENT_NUMBER' does not exist, it returns '0'
                   'validation_override': True,
                   'uuid': appuuid
                   }
        response_sqs = send_sqs_message(message)
        if response_sqs is None:
            raise ValueError('Could not send message to SQS')
        print(f'Message sent to SQS: {response_sqs}')
    
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
    This function is the AWS Lambda function call for DocumentLambdaFunction.

    Parameters:

    event: S3 event, which contains bucket name and filename with the prefix
    context: not used in this application

    Returns:
    
    """    
    
    print(f'Entering lambda handler for DocumentLambdaFunction')
    
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    LAMBDA_TMP_FOLDER = '/tmp/'
    LAMBDA_UNZIPPED_FOLDER = 'unzipped/'
    
    ret = False

    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f'record: {record}')
        print(f'bucket: {bucket}') # e.g. bucket: documentbucket-115476135777
        print(f'key: {key}')  # e.g. key: zipped/8d247914.zip
        
        #====================================================================================
        # Get .zip file from S3 bucket, unzip the file, then store the unzipped objects in S3
        #====================================================================================
        customer_info = {'selfie_key' : '', 'license_key' : '', 'details_file' : '', 'appuuid' : ''}
        valerror = {'error':''}
        outcome = prepare_customer_info(bucket, key, LAMBDA_TMP_FOLDER, LAMBDA_UNZIPPED_FOLDER, BUCKET_UNZIPPED_PREFIX, customer_info, valerror)
        if outcome == False:
            raise ValueError('Error in prepare_customer_info')

        selfie_key = customer_info['selfie_key']
        license_key = customer_info['license_key']
        details_file = customer_info['details_file']
        appuuid = customer_info['appuuid']

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

        #=======================================================================================================
        # Compare customer's selfie image with the image in the customer's driver license using AWS Rekognition.
        # Update DynamoDB table with the outcome of this comparison.
        # Send an email if the comparison fails.
        #=======================================================================================================
        valerror = {'error':''}
        outcome = validate_selfie(bucket, selfie_key, license_key, appuuid, ddb_table, valerror)
        if outcome == False:
            raise ValueError('Error in validate_selfie')
        
        #=====================================================================================================
        # Compare customer's submitted info (in details_dic) with customer's driver license using AWS Textract.
        # Update DynamoDB table the outcome of this comparison.
        # Send an email if the comparison fails.
        #=====================================================================================================
        outcome = validate_customer_details(bucket, license_key, appuuid, ddb_table, details_dic)
        if outcome == False:
            raise ValueError('Error in validate_customer_details')
        
        #==============================================================================================
        # Write customer's license number (available in details_dic) to Amazon SQS queue.
        # When a new message is in the queue, another Lambda function named SubmitLicenseLambdaFunction
        # will be invoked, which in turn will submit the license ID to the third-party API for validation.
        #==============================================================================================
        outcome = queue_customer_id(appuuid, details_dic)
        if outcome == False:
            raise ValueError('Error in queue_customer_id')
        
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

