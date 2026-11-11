# AWS Serverless ETL Pipeline – Cancer Data Processing

## Project overview

This project demonstrates an event-driven, serverless data pipeline built with AWS. It automates file-upload detection, email notifications, ETL processing, and storage of processed data.

A file is uploaded to an Amazon S3 extraction folder. The configured event workflow sends an email notification through Amazon SNS and invokes AWS Lambda to start an AWS Glue job. Glue reads the source CSV data, removes duplicate records, evaluates a basic data-quality rule, and writes the result to an S3 load folder.

## AWS services and technologies

- **Amazon S3** – stores source and processed data.
- **AWS Lambda** – runs Python code to start the Glue job.
- **AWS Glue** – performs the ETL process using a DynamicFrame and Spark.
- **Amazon EventBridge** – supports event routing and event-driven automation in the workflow.
- **Amazon SNS** – sends email notifications when a file is uploaded.
- **Amazon CloudWatch** – stores logs used to monitor executions and troubleshoot issues.
- **Python / Boto3 / PySpark** – used for Lambda automation and Glue data processing.

## Architecture and workflow

1. A CSV file is uploaded to the S3 `extract/` folder.
2. The configured S3/EventBridge event workflow detects the upload.
3. Amazon SNS sends an email notification about the uploaded file.
4. AWS Lambda is invoked and calls `glue.start_job_run()` for the Glue job named `s3-cancer`.
5. AWS Glue reads CSV data from the S3 extraction location.
6. Glue removes duplicate rows using `dropDuplicates()`.
7. AWS Glue Data Quality evaluates the rule `ColumnCount > 0`.
8. The processed data is written to the S3 `load/` folder in CSV format.
9. CloudWatch logs provide execution information for monitoring and troubleshooting.

## Repository structure

```text
aws-cancer-data-etl-pipeline/
├── lambda/
│   └── lambda_function.py
├── glue/
│   └── glue_etl.py
├── docs/
│   └── (add architecture diagram or AWS screenshots here)
├── README.md
└── .gitignore
```

## Pipeline configuration

| Component | Configuration |
|---|---|
| Glue job name | `s3-cancer` |
| Lambda input argument | `s3://learningsaws-etl/extract/` |
| Lambda output argument | `s3://learningaws-etl/load/` |
| Glue script input | `s3://learningaws-etl/extract/` |
| Glue script output | `s3://learningaws-etl/load/` |
| Transformation | Remove duplicate rows |
| Data quality rule | `ColumnCount > 0` |
| Glue output format | CSV |

> **Configuration check before running:** The Lambda code currently passes `s3://learningsaws-etl/extract/` (with an extra `s` in `learningsaws`), while the Glue script reads from `s3://learningaws-etl/extract/`. Confirm the real bucket name and make both configurations consistent.

> **Important implementation detail:** The current Lambda passes `--S3_INPUT_PATH` and `--S3_OUTPUT_PATH` as Glue job arguments, but the current Glue script only reads `JOB_NAME` and uses hardcoded S3 paths. Therefore, those two Lambda arguments are not currently used by the Glue script. The hardcoded Glue paths determine where the job reads and writes.

## Key implementation

### Lambda

The Lambda function uses Boto3 to start the Glue job:

```python
glue.start_job_run(
    JobName='s3-cancer',
    Arguments={
        '--S3_INPUT_PATH': 's3://learningsaws-etl/extract/',
        '--S3_OUTPUT_PATH': 's3://learningaws-etl/load/'
    }
)
```

### Glue transformation

The Glue job removes duplicate records:

```python
DropDuplicates_node = DynamicFrame.fromDF(
    source_frame.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node"
)
```

It then evaluates a basic data-quality rule and writes the processed DynamicFrame to S3.

## Skills demonstrated

- AWS cloud services
- Serverless and event-driven architecture
- Python and Boto3
- ETL pipeline development
- AWS Glue and PySpark
- Data deduplication and data quality checks
- S3 data organization
- Cloud monitoring and logging
- Event notifications with SNS

## Project outcome

Built an automated AWS data pipeline that responds to file uploads, sends email notifications, starts a Glue ETL job, removes duplicate records, evaluates a basic data-quality rule, and stores processed output in Amazon S3.

## Screenshots to add

For a stronger portfolio presentation, add screenshots showing:

1. S3 `extract/` and `load/` folders.
2. Lambda function and successful execution.
3. Glue job script and successful job run.
4. CloudWatch execution logs.
5. EventBridge rule or event configuration.
6. SNS email notification received.

## Security notes

- Do not commit AWS access keys, secret keys, passwords, or credentials.
- Do not upload private or sensitive patient-level data.
- Use screenshots with account IDs, email addresses, and other sensitive details hidden.
