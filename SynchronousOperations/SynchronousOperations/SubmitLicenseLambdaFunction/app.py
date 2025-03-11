import os
import boto3
import json
import requests

SNS_LICENSEVALIDATION_MESSAGE = 'Invalid Customer\'s license'
SNS_LICENSEVALIDATION_SUBJECT = 'Customer\'s License Validation Fails'

dynamoDb = boto3.resource('dynamodb')
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
    
def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for SubmitLicenseLambdaFunction.

    Parameters:

    event: SQS message, which was written by DocumentLambdaFunction.
           See queue_customer_id() in DocumentLambdaFunction.
    context: not used in this application

    Returns:
    
    """    
    ret = False

    try:
        url = os.environ['INVOKE_URL']

        record = event['Records'][0]
        body = record['body'] # According to the sample event message, the body is a string value.
        payload = json.loads(body)
        driver_license_id = payload['driver_license_id']
        validation_override = payload['validation_override']
        appuuid = payload['uuid']
        
        print(f'record= {record}')
        print(f'body= {body}')
        print(f'payload= {payload}')
        print(f'driver_license_id= {driver_license_id}')
        print(f'validation_override= {validation_override}')
        print(f'appuuid= {appuuid}')
        
        # Submit the driver_license_id and the validation_override to the third-party API.
        # Then wait for the third-party API to return a response.
        # For more information on HTTP Post request, see:
        # https://requests.readthedocs.io/en/latest/user/quickstart/#make-a-request
        third_party_response = requests.post(url, json=payload)

        #=======================================================
        # The response comes from ValidateLicenseLambdaFunction
        #=======================================================

        print(f'third_party_response = {third_party_response}')
        print(f'requests.codes.ok = {requests.codes.ok}')
        print(f'third_party_response.status_code = {third_party_response.status_code}')

        if third_party_response.status_code != requests.codes.ok:
            raise ValueError('Error is HTTP Response')
        
        response_in_json = third_party_response.json()
        print(f'response_in_json = {response_in_json}')

        # If the response is true, then:
        # - Update DynamoDB table for the given APP_UUID by setting LICENSE_VALIDATION to TRUE

        # If the response is false, then:
        # - Update DynamoDB table for the given APP_UUID by setting LICENSE_VALIDATION to FALSE
        # - Send a failure message to SNS topic
    
        #  The DynamoDB update_item() will create the attribute if it does not exist.
        ddb_table_name = get_dynamo_db_table_name()
        if ddb_table_name is None:
            raise ValueError('No DynamoDB table')
        print(f'ddb_table_name: {ddb_table_name}')
            
        ddb_table = dynamoDb.Table(ddb_table_name)
        if ddb_table is None:
            raise ValueError('No DynamoDB table')
        print(f'ddb_table: {ddb_table}')
        
        response_db_update = ddb_table.update_item(
            Key={"APP_UUID": appuuid},
            UpdateExpression='SET LICENSE_VALIDATION=:v_matches',
            ExpressionAttributeValues={':v_matches':response_in_json})
        if response_db_update is None:
            raise ValueError('Could not update DynamoDB Table item with LICENSE_VALIDATION')
        print(f'Response to update LICENSE_VALIDATION attribute: {response_db_update}')
            
        # Send SNS email if a match is not found, and then raise an exception
        if not response_in_json:
            # Send SNS
            send_sns_email(SNS_LICENSEVALIDATION_MESSAGE, SNS_LICENSEVALIDATION_SUBJECT)
            raise ValueError('Could not validate Customer\'s license')
            
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

