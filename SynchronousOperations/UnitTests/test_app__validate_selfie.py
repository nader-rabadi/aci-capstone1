import unittest
from unittest.mock import patch
import boto3
from moto import mock_aws
import sys
import os
import shutil
import zipfile

# Append the path to sys.path, in order to import from DocumentLambdaFunction/
path_to_add = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path_to_add)

from SynchronousOperations.DocumentLambdaFunction.app import validate_selfie
from SynchronousOperations.DocumentLambdaFunction.app import SIMILARITY_THRESHOLD
from SynchronousOperations.DocumentLambdaFunction.app import s3
from SynchronousOperations.DocumentLambdaFunction.app import dynamodb
from SynchronousOperations.DocumentLambdaFunction.app import rekognition
from SynchronousOperations.DocumentLambdaFunction.app import sns

class TestRekognition(unittest.TestCase):

    BUCKET_NAME = 'documentbucket-123456789102'
    LAMBDA_TMP_FOLDER = '\\tmp\\'
    APPUUID = '8d247914'
    
    def setUp(self):
        # Create \\tmp\\ folder to mimic Lambda's internal memory
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        directory_path = project_dir+'\\UnitTests'+TestRekognition.LAMBDA_TMP_FOLDER

        if os.path.isdir(directory_path) is False:
            os.mkdir(directory_path)
                
    def tearDown(self):
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        directory_path = project_dir+'\\UnitTests'+TestRekognition.LAMBDA_TMP_FOLDER
        if os.path.isdir(directory_path):
            shutil.rmtree(directory_path)

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @mock_aws
    def test_validate_selfie(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)
        patch_resource(dynamodb)
        patch_client(rekognition)
        
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
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestRekognition.BUCKET_NAME)

        source_image = 'fakesource.png'
        target_image = 'faketarget.png'

        # Mock response for compare_faces - indicating a match.
        # AWS Lambda checks against SIMILARITY_THRESHOLD. A match must be greater than SIMILARITY_THRESHOLD.
        # Hence, I added +1 below to get a match
        rekognition_mock_response = {
            'FaceMatches': [
                {'Similarity': SIMILARITY_THRESHOLD + 1,
                 'Face':{'BoundingBox':{'Width': 0.5, 'Height': 0.3, 'Left': 0.2, 'Top': 0.1},
                         'Confidence': 99.9}
                }
            ],
            'UnmatchedFaces': [],
            'ResponseMetadata': {'RequestId': 'example-request-id', 'HTTPStatusCode': 200}
        }

        # The moto library mocks AWS services, so you can return this response
        # when calling the `compare_faces` method
        rekognition.compare_faces = lambda **kwargs: rekognition_mock_response

        valerror = {'error':''}

        # Call the function to test
        response = validate_selfie(TestRekognition.BUCKET_NAME,
                                   source_image,
                                   target_image,                                   
                                   TestRekognition.APPUUID,
                                   table,
                                   valerror)
        
        # Assert selfie validation passed
        self.assertEqual(response, True)
        
        # Verify that the item was indeed inserted into the table
        table = dynamodb.Table('test_table')
        response = table.get_item(Key={'APP_UUID': TestRekognition.APPUUID})
        
        # Assert the item  LICENSE_SELFIE_MATCH was added successfully
        self.assertIn('Item', response)
        self.assertEqual(response['Item'], {'LICENSE_SELFIE_MATCH': True} | {'APP_UUID': TestRekognition.APPUUID})

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @patch.dict(os.environ, {'TOPIC': 'test_topic'})
    @mock_aws
    def test_low_similarity_match_invalid_selfie(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)
        patch_resource(dynamodb)
        patch_client(rekognition)
        patch_client(sns)
        
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
                
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestRekognition.BUCKET_NAME)

        source_image = 'fakesource.png'
        target_image = 'faketarget.png'

        # Mock response for compare_faces - indicating no match due to lower similarity than expected.
        # S3 Lambda checks against SIMILARITY_THRESHOLD. A match must be greater than SIMILARITY_THRESHOLD.
        # Hence, I subtract -1 below to get a no match
        rekognition_mock_response = {
            'FaceMatches': [
                {'Similarity': SIMILARITY_THRESHOLD - 1,
                 'Face':{'BoundingBox':{'Width': 0.5, 'Height': 0.3, 'Left': 0.2, 'Top': 0.1},
                         'Confidence': 99.9}
                }
            ],
            'UnmatchedFaces': [],
            'ResponseMetadata': {'RequestId': 'example-request-id', 'HTTPStatusCode': 200}
        }

        # The moto library mocks AWS services, so you can return this response
        # when calling the `compare_faces` method
        rekognition.compare_faces = lambda **kwargs: rekognition_mock_response

        # Mock response for SNS publish()
        sns_mock_response = {
            'MessageId': 'test_id',
            'SequenceNumber': '12345'
        }

        sns.publish = lambda **kwargs: sns_mock_response

        valerror = {'error':''}

        # Call the function to test
        response = validate_selfie(TestRekognition.BUCKET_NAME,
                                   source_image,
                                   target_image,                                   
                                   TestRekognition.APPUUID,
                                   table,
                                   valerror)
        
        # Assert selfie validation failed
        self.assertEqual(response, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'Could not match selfie with license')
        
        # Verify that the item was indeed inserted into the table
        table = dynamodb.Table('test_table')
        response = table.get_item(Key={'APP_UUID': TestRekognition.APPUUID})
        
        # Assert the item  LICENSE_SELFIE_MATCH was added successfully, and is set to False
        self.assertIn('Item', response)
        self.assertEqual(response['Item'], {'LICENSE_SELFIE_MATCH': False} | {'APP_UUID': TestRekognition.APPUUID})

    @patch.dict(os.environ, {'TABLE': 'test_table'})
    @patch.dict(os.environ, {'TOPIC': 'test_topic'})
    @mock_aws
    def test_no_matching_faces_invalid_selfie(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource 
        patch_client(s3)
        patch_resource(dynamodb)
        patch_client(rekognition)
        patch_client(sns)
                
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
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestRekognition.BUCKET_NAME)
            
        source_image = 'fakesource.png'
        target_image = 'faketarget.png'

        # Mock response for compare_faces - indicating no match
        rekognition_mock_response = {
            'FaceMatches': [],
            'UnmatchedFaces': [
                {
                    'BoundingBox': {
                        'Width': 0.1,
                        'Height': 0.1,
                        'Left': 0.1,
                        'Top': 0.1
                    },
                    'Confidence': 99.0
                }
            ],
            'ResponseMetadata': {'RequestId': 'example-request-id', 'HTTPStatusCode': 200}
        }

        # The moto library mocks AWS services, so you can return this response
        # when calling the `compare_faces` method
        rekognition.compare_faces = lambda **kwargs: rekognition_mock_response

        # Mock response for SNS publish()
        sns_mock_response = {
            'MessageId': 'test_id',
            'SequenceNumber': '12345'
        }

        sns.publish = lambda **kwargs: sns_mock_response

        valerror = {'error':''}

        # Call the function to test
        response = validate_selfie(TestRekognition.BUCKET_NAME,
                                   source_image,
                                   target_image,
                                   TestRekognition.APPUUID,
                                   table,
                                   valerror)
        
        # Assert selfie validation failed
        self.assertEqual(response, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'Could not match selfie with license')
        
        # Verify that the item was indeed inserted into the table
        table = dynamodb.Table('test_table')
        response = table.get_item(Key={'APP_UUID': TestRekognition.APPUUID})
        
        # Assert the item  LICENSE_SELFIE_MATCH was added successfully, and is set to False
        self.assertIn('Item', response)
        self.assertEqual(response['Item'], {'LICENSE_SELFIE_MATCH': False} | {'APP_UUID': TestRekognition.APPUUID})

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



