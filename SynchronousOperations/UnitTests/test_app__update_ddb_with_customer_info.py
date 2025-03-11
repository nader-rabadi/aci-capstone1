import unittest
from unittest.mock import patch
import boto3
from moto import mock_aws
import sys
import os

# Append the path to sys.path, in order to import from DocumentLambdaFunction/
path_to_add = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path_to_add)

from SynchronousOperations.DocumentLambdaFunction.app import update_ddb_with_customer_info
from SynchronousOperations.DocumentLambdaFunction.app import dynamodb

class TestDynamoDB(unittest.TestCase):

    APPUUID = '8d247914'
    CUSTOMER_DETAILS_FILE = '8d247914_details.csv'

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @mock_aws
    def test_add_customer_details_to_dynamodb(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource
        patch_resource(dynamodb)
        
        # Create a mock table
        table = dynamodb.create_table(
            TableName='test_table',
            KeySchema=[
                {
                    'AttributeName': 'APP_UUID',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'APP_UUID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        
        customer_details = {'ddb_table':'', 'details_dic':''}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}

        # Construct the absolute path to the file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestDynamoDB.CUSTOMER_DETAILS_FILE)

        # Call the function to test
        response = update_ddb_with_customer_info(file_path, TestDynamoDB.APPUUID, customer_details, ddb_response, valerror)
        
        # Assert the item was added successfully
        self.assertEqual(response, True)
        self.assertEqual(ddb_response['ddb_response']['ResponseMetadata']['HTTPStatusCode'], 200)
        
        # Verify that the item was indeed inserted into the table
        table = dynamodb.Table('test_table')
        response = table.get_item(Key={'APP_UUID': TestDynamoDB.APPUUID})
        
        # Assert that the item exists in the table
        self.assertIn('Item', response)
        self.assertEqual(response['Item'], customer_details['details_dic'] | {'APP_UUID': TestDynamoDB.APPUUID})


    @patch.dict(os.environ, {'TABLE': ''})
    @mock_aws
    def test_exception_no_table_customer_details_to_dynamodb(self):

        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource
        patch_resource(dynamodb)
        
        # Create a mock table
        table = dynamodb.create_table(
            TableName='test_table',
            KeySchema=[
                {
                    'AttributeName': 'APP_UUID',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'APP_UUID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        
        customer_details = {'ddb_table':'', 'details_dic':''}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}

        # Construct the absolute path to the .csv file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestDynamoDB.CUSTOMER_DETAILS_FILE)

        # Call the function to test
        response = update_ddb_with_customer_info(file_path, TestDynamoDB.APPUUID, customer_details, ddb_response, valerror)
        
        # Assert the ValueError occured
        self.assertEqual(response, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'No DynamoDB table')
     
    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @mock_aws
    def test_exception_nonexisting_table_customer_details_to_dynamodb(self):

        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')
        
        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource
        patch_resource(dynamodb)

        # Save the original Table function
        original_dynamodb_table = dynamodb.Table

        customer_details = {'ddb_table':'', 'details_dic':''}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}

        # Construct the absolute path to the .csv file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestDynamoDB.CUSTOMER_DETAILS_FILE)

        # Mock response for dynamodb.Table - indicating a None.
        dynamodb_mock_response = None

        # The moto library mocks AWS services, so you can return this response
        # when calling the `dynamodb.Table` method        
        dynamodb.Table = lambda table_name: dynamodb_mock_response

        # Call the function to test
        response = update_ddb_with_customer_info(file_path, TestDynamoDB.APPUUID, customer_details, ddb_response, valerror)
        
        # Assert the ValueError occured
        self.assertEqual(response, False)
        self.assertIn('Table: test_table not found', str(valerror['error']))

        # Un-mock the Table method for subsequent tests
        dynamodb.Table = original_dynamodb_table

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @mock_aws
    def test_exception_nonexisting_csvfile_customer_details_to_dynamodb(self):

        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource
        patch_resource(dynamodb)
        
        # Create a mock table
        table = dynamodb.create_table(
            TableName='test_table',
            KeySchema=[
                {
                    'AttributeName': 'APP_UUID',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'APP_UUID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )

        customer_details = {'ddb_table':'', 'details_dic':''}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}

        # Construct the absolute path to the .csv file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', 'dummayfile.nonexist')

        # Call the function to test
        response = update_ddb_with_customer_info(file_path, TestDynamoDB.APPUUID, customer_details, ddb_response, valerror)

        # Assert the ValueError occured
        self.assertEqual(response, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'Could not parse csv file')

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @mock_aws
    def test_failed_adding_customer_details_to_dynamodb(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource
        patch_resource(dynamodb)
        
        # Create a mock table
        table = dynamodb.create_table(
            TableName='test_table',
            KeySchema=[
                {
                    'AttributeName': 'UnknownID',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'UnknownID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        
        customer_details = {'ddb_table':'', 'details_dic':''}
        ddb_response = {'ddb_response':''}
        valerror = {'error':''}

        # Construct the absolute path to the file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestDynamoDB.CUSTOMER_DETAILS_FILE)

        # Call the function to test
        response = update_ddb_with_customer_info(file_path, TestDynamoDB.APPUUID, customer_details, ddb_response, valerror)
        
        # Assert the item was not added
        self.assertEqual(response, False)
        # ddb_response = {'ddb_response': ''}
        self.assertTrue(not any(ddb_response.values()))
        self.assertIn('Missing the key UnknownID in the item', str(valerror['error']))


if __name__ == '__main__':

    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    unittest.main()

    # Remove the same path from sys.path when finished testing
    if path_to_add in sys.path:
        sys.path.remove(path_to_add)

