from aws_cdk import Stack, Duration, RemovalPolicy
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda_event_sources as eventsources
from constructs import Construct

class CdkIacProjectStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Create the S3 Bucket
        bucket = s3.Bucket(self, "WebsiteTrafficBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # 2. Create the Kinesis Data Stream
        stream = kinesis.Stream(self, "WebsiteClickStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND
        )

        # 3. Create the Lambda Execution Role
        lambda_role = iam.Role(self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisReadOnlyAccess")
            ]
        )

        # 4. Add custom S3 write policy to the role
        bucket.grant_write(lambda_role)

        # 5. Create the Lambda Function
        lambda_function = lambda_.Function(self, "KinesisToS3Function",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=lambda_.Code.from_inline(f"""
import json
import boto3
import base64
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    current_date = datetime.now().strftime('%Y-%m-%d')
    filename = f'website-data/{{current_date}}/traffic.log'
    
    records = []
    for record in event['Records']:
        payload = record['kinesis']['data']
        decoded_bytes = base64.b64decode(payload)
        decoded_string = decoded_bytes.decode('utf-8')
        records.append(decoded_string + '\\n')
    
    data_to_write = ''.join(records)
    
    s3_client.put_object(
        Bucket='{bucket.bucket_name}',
        Key=filename,
        Body=data_to_write
    )
    
    return {{'statusCode': 200, 'body': json.dumps('Success!')}}
"""),
            role=lambda_role,
            timeout=Duration.seconds(30)
        )

        # 6. Add Kinesis as a trigger to Lambda
        lambda_function.add_event_source(eventsources.KinesisEventSource(stream,
            batch_size=100,
            starting_position=lambda_.StartingPosition.LATEST
        ))

        # Output useful information
        self.output_props = {
            "bucket_name": bucket.bucket_name,
            "stream_name": stream.stream_name,
            "lambda_function_name": lambda_function.function_name
        }
