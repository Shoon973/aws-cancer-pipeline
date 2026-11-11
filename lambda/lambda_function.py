import json
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):

    print(event)
    print("Lambda got Triggered!!")

    response = glue.start_job_run(
        JobName='s3-cancer',
        Arguments={
            '--S3_INPUT_PATH': 's3://learningsaws-etl/extract/',
            '--S3_OUTPUT_PATH': 's3://learningaws-etl/load/'
        }
    )

    print("Glue Job Started!")
    print("Job Run ID:", response['JobRunId'])

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Glue job triggered successfully!',
            'JobRunId': response['JobRunId']
        })
    }
