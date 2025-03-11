import os
import boto3

SIMILARITY_THRESHOLD = 80
SNS_FACEMATCH_MESSAGE = 'No matches between selfie and license'
SNS_FACEMATCH_SUBJECT = 'Face Match Fails'

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
sns = boto3.client('sns')

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


def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for CompareFacesLambdaFunction.

    Parameters:

    event: State event, which contains app_uuid and bucket name
    context: not used in this application

    Returns:
    
    A dictionary that contains a status as either failure or success,
    with a message describing the status.
    
    """    
    
    print(f'Entering lambda handler for CompareFacesLambdaFunction')
    
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    
    ret = {
        "status": "failure",
        "message": "Selfie Comparison failed"
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

        selfie_key = BUCKET_UNZIPPED_PREFIX + appuuid + '_selfie.png'
        license_key = BUCKET_UNZIPPED_PREFIX + appuuid + '_license.png'

        #====================================================================
        # Get DynamoDB table. The status of Rekognition comparison operation
        # will be written in the table.
        #====================================================================
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
                        
        #=======================================================================================================
        # Compare customer's selfie image with the image in the customer's driver license using AWS Rekognition.
        # Update DynamoDB table with the outcome of this comparison.
        # Send an email if the comparison fails.
        #=======================================================================================================
        valerror = {'error':''}
        outcome = validate_selfie(bucket, selfie_key, license_key, appuuid, ddb_table, valerror)
        if outcome == False:
            raise ValueError('Error in validate_selfie')
                        
    except Exception as error:
        print(f'Exception error: {error}')
        
    else:
        # If no errors are detected, continue to execute the following:
        print(f'else block: do nothing for now')
        
        ret = {
            "status": "success",
            "message": "Selfie Comparison successful"
        }
        
    finally:
        # Execute the following code whether or not an exception has been raised:
        print(f'finally block: do nothing for now')

        return ret

