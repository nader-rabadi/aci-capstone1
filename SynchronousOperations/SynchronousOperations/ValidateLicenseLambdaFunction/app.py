import json

def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for ValidateLicenseLambdaFunction.
    It takes API gateway event and responds with the validation_override.
    In other words, this is a fake driver license API that responds with the validation_override.

    Parameters:

    event: API Gateway event which contains driver_license_id and validation_override.
    context: not used in this application

    Returns:
    
    A dictionary with HTTP status code of 200, and
    a body message with the received override_parameter.
    
    """ 
    body = event['body']
    body_json = json.loads(body)
    license_id = body_json['driver_license_id']
    override_parameter = body_json['validation_override']

    response = {}
    response['statusCode'] = 200
    response['body'] = override_parameter
    return response