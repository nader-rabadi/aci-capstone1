import unittest
from unittest import mock
import boto3
from moto import mock_aws
import sys
import os
import shutil

# Append the path to sys.path, in order to import from DocumentLambdaFunction/
path_to_add = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path_to_add)

from SynchronousOperations.DocumentLambdaFunction.app import prepare_customer_info
from SynchronousOperations.DocumentLambdaFunction.app import s3

class TestS3ApplicationData(unittest.TestCase):

    ZIPFILE = '8d247914.zip'
    BUCKET_NAME = 'documentbucket-123456789102'
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    LAMBDA_TMP_FOLDER = '\\tmp\\'
    LAMBDA_UNZIPPED_FOLDER = 'unzipped\\'

    APPUUID = '8d247914'
    CUSTOMER_DETAILS_FILE = LAMBDA_TMP_FOLDER + LAMBDA_UNZIPPED_FOLDER + APPUUID + '_details.csv'
    LICENSE_KEY_FILE = BUCKET_UNZIPPED_PREFIX + APPUUID + '_license.png'
    SELFIE_KEY_FILE = BUCKET_UNZIPPED_PREFIX + APPUUID + '_selfie.png'

    def setUp(self):
        # Create \\tmp\\ folder to mimic Lambda's internal memory
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        directory_path = project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER
        if os.path.isdir(directory_path) is False:
            os.mkdir(directory_path)
    
    def tearDown(self):
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        directory_path = project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER
        if os.path.isdir(directory_path):
            shutil.rmtree(directory_path)
    
    @mock_aws
    def test_add_unzipped_objects_to_s3(self):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)

        # Construct the absolute path to the zip file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestS3ApplicationData.ZIPFILE)
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestS3ApplicationData.BUCKET_NAME)

        # Store .zip file in zipped/ prefix
        zipped_prefix = "zipped/"
        file_name = TestS3ApplicationData.ZIPFILE
        object_key = f"{zipped_prefix}{file_name}"

        # Upload the zip file to the "zipped" prefix
        s3.upload_file(file_path, TestS3ApplicationData.BUCKET_NAME, object_key)
        
        customer_info = {'selfie_key' : '', 'license_key' : '', 'details_file' : '', 'appuuid' : ''}
        valerror = {'error':''}

        # Call the function to test
        ret = prepare_customer_info(TestS3ApplicationData.BUCKET_NAME,
                                    object_key,
                                    project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER,
                                    TestS3ApplicationData.LAMBDA_UNZIPPED_FOLDER,
                                    TestS3ApplicationData.BUCKET_UNZIPPED_PREFIX,
                                    customer_info,
                                    valerror)

        # Assert that objects were added
        self.assertEqual(ret, True)

        # Assert the following are set
        self.assertEqual(customer_info['selfie_key'], TestS3ApplicationData.SELFIE_KEY_FILE)
        self.assertEqual(customer_info['license_key'], TestS3ApplicationData.LICENSE_KEY_FILE)
        self.assertEqual(customer_info['details_file'], project_dir+'\\UnitTests'+TestS3ApplicationData.CUSTOMER_DETAILS_FILE)
        self.assertEqual(customer_info['appuuid'], TestS3ApplicationData.APPUUID)

    @mock_aws
    @mock.patch('SynchronousOperations.DocumentLambdaFunction.app.unzip_file')
    def test_failedunzipping_adding_unzipped_objects_to_s3(self, unzip_file_mock):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)

        unzip_file_mock.return_value = False

        # Construct the absolute path to the zip file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestS3ApplicationData.ZIPFILE)
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestS3ApplicationData.BUCKET_NAME)

        # Store .zip file in zipped/ prefix
        zipped_prefix = "zipped/"
        file_name = TestS3ApplicationData.ZIPFILE
        object_key = f"{zipped_prefix}{file_name}"

        # Upload the zip file to the "zipped" prefix
        s3.upload_file(file_path, TestS3ApplicationData.BUCKET_NAME, object_key)
        
        customer_info = {'selfie_key' : '', 'license_key' : '', 'details_file' : '', 'appuuid' : ''}
        valerror = {'error':''}

        # Call the function to test
        ret = prepare_customer_info(TestS3ApplicationData.BUCKET_NAME,
                                    object_key,
                                    project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER,
                                    TestS3ApplicationData.LAMBDA_UNZIPPED_FOLDER,
                                    TestS3ApplicationData.BUCKET_UNZIPPED_PREFIX,
                                    customer_info,
                                    valerror)

        # Assert that objects were not added
        self.assertEqual(ret, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'Error while unzipping a file')

    @mock_aws
    @mock.patch('SynchronousOperations.DocumentLambdaFunction.app.get_unzipped_files')
    def test_failednofiles_adding_unzipped_objects_to_s3(self, get_unzipped_files_mock):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)

        get_unzipped_files_mock.return_value = None

        # Construct the absolute path to the zip file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestS3ApplicationData.ZIPFILE)
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestS3ApplicationData.BUCKET_NAME)

        # Store .zip file in zipped/ prefix
        zipped_prefix = "zipped/"
        file_name = TestS3ApplicationData.ZIPFILE
        object_key = f"{zipped_prefix}{file_name}"

        # Upload the zip file to the "zipped" prefix
        s3.upload_file(file_path, TestS3ApplicationData.BUCKET_NAME, object_key)
        
        customer_info = {'selfie_key' : '', 'license_key' : '', 'details_file' : '', 'appuuid' : ''}
        valerror = {'error':''}

        # Call the function to test
        ret = prepare_customer_info(TestS3ApplicationData.BUCKET_NAME,
                                    object_key,
                                    project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER,
                                    TestS3ApplicationData.LAMBDA_UNZIPPED_FOLDER,
                                    TestS3ApplicationData.BUCKET_UNZIPPED_PREFIX,
                                    customer_info,
                                    valerror)

        # Assert that objects were not added
        self.assertEqual(ret, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'No files to upload to S3')

    @mock_aws
    @mock.patch('SynchronousOperations.DocumentLambdaFunction.app.upload_file_to_s3')
    def test_failedupload_adding_unzipped_objects_to_s3(self, upload_file_to_s3_mock):
        print(f'***************************************************')
        print(f'Unit Test: {self.__class__.__name__} : {self._testMethodName} :')
        print(f'***************************************************')

        # https://docs.getmoto.org/en/latest/docs/getting_started.html
        # According to moto documentation, I can use the clients and resources that I created
        # in AWS Lambda function, and then patch them (using patch_client() and patch_resource())
        # to be used with moto.
        from moto.core import patch_client, patch_resource    
        patch_client(s3)

        upload_file_to_s3_mock.return_value = False

        # Construct the absolute path to the zip file located in UnitTests/
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = os.path.join(project_dir, 'UnitTests', TestS3ApplicationData.ZIPFILE)
        
        # Create a mock S3 bucket
        s3.create_bucket(Bucket=TestS3ApplicationData.BUCKET_NAME)

        # Store .zip file in zipped/ prefix
        zipped_prefix = "zipped/"
        file_name = TestS3ApplicationData.ZIPFILE
        object_key = f"{zipped_prefix}{file_name}"

        # Upload the zip file to the "zipped" prefix
        s3.upload_file(file_path, TestS3ApplicationData.BUCKET_NAME, object_key)
        
        customer_info = {'selfie_key' : '', 'license_key' : '', 'details_file' : '', 'appuuid' : ''}
        valerror = {'error':''}

        # Call the function to test
        ret = prepare_customer_info(TestS3ApplicationData.BUCKET_NAME,
                                    object_key,
                                    project_dir+'\\UnitTests'+TestS3ApplicationData.LAMBDA_TMP_FOLDER,
                                    TestS3ApplicationData.LAMBDA_UNZIPPED_FOLDER,
                                    TestS3ApplicationData.BUCKET_UNZIPPED_PREFIX,
                                    customer_info,
                                    valerror)

        # Assert that objects were not added
        self.assertEqual(ret, False)
        # Use either str() or .args[0] to get the string inside ValueError().
        self.assertEqual(valerror['error'].args[0], 'Error in uploading a file to S3')

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
