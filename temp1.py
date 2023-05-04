# function to upload a file to an S3 bucket and connect to the database


import boto3
import os
import json

def upload_file(file_name, bucket):
    object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)

    return response


def connect_to_database():
    client = boto3.client('rds-data')
    return client


def execute_statement(client, sql):
    response = client.execute_statement(
        secretArn=os.environ['SECRET_ARN'],
        database=os.environ['DATABASE'],
        resourceArn=os.environ['RESOURCE_ARN'],
        sql=sql
    )

    return response


def lambda_handler(event, context):
    print(event)
    file_name = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    sql = "SELECT * FROM employees"

    client = connect_to_database()
    response = execute_statement(client, sql)
    print(response)

    upload_file(file_name, bucket)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

#unit test cases for upload_file function

import unittest

class TestUploadFile(unittest.TestCase):
    def test_upload_file(self):
        self.assertEqual(upload_file('test.txt', 'test-bucket'), None)
    
    def test_upload_file_exception(self):
        self.assertRaises(Exception, upload_file('test.txt', 'test-bucket'))

