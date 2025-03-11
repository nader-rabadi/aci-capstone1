import os
import botocore
import boto3
import zipfile

s3 = boto3.client('s3')

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
    
def lambda_handler(event, context):
    """
    This function is the AWS Lambda function call for UnzipLambdaFunction.

    Parameters:

    event: EventBridge rule that invokes State Machine, which contains bucket name and filename with the prefix
    context: not used in this application

    Returns:
    
    A dictionary: {"app_uuid":appuuid}. Otherwise, None.

    """    
    
    print(f'Entering lambda handler for UnzipLambdaFunction')
    
    BUCKET_UNZIPPED_PREFIX = 'unzipped/'
    LAMBDA_TMP_FOLDER = '/tmp/'
    LAMBDA_UNZIPPED_FOLDER = 'unzipped/'
    
    ret = None

    try:
        record = event['detail']
        bucket = record['bucket']['name']
        key = record['object']['key']

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

        response = {"app_uuid":appuuid}
    
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

