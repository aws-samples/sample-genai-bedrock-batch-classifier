import os
import logging
from utils.dynamodb import get_job_status_items
from batchResultsProcessing.dataProcessor import DataProcessor
from batchResultsProcessing.environmentConfig import EnvironmentConfig
from utils.sqs_parser import extract_bucket_from_sqs_message
from utils.s3 import read_s3_file

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


def lambda_handler(event, context):
    """
    AWS Lambda handler for processing batch classification results.

    Args:
        event: Lambda event object
        context: Lambda context object
    """
    try:
        logger.info("Start data batch results processing.")

        # Initialize configuration
        config = EnvironmentConfig()
        processor = DataProcessor(config)

        for record in event["Records"]:
            bucket = extract_bucket_from_sqs_message(record["body"])

            if not bucket:
                logger.error("Failed to extract bucket information")
                continue

            input_bucket_name = bucket.get("input_bucket_name")
            input_key_name = bucket.get("input_key_name")
            bedrock_job_short_id = input_key_name.split("/")[-2]

            # Find the record in the DynamoDB
            response = get_job_status_items(
                config.get("job_status_table"),
                {"bedrock_job_short_id": bedrock_job_short_id},
                page_size=25,
                consistent_read=True
            )

            if response:
                job = response[0]
                file_name = job["id"]["S"]
                parent_job_id = job["parent_id"]["S"]
                logger.info(f"Found a job with id '{file_name}' for Bedorck job '{bedrock_job_short_id}'")

                # Read input file
                content = read_s3_file(input_bucket_name, input_key_name)
                if not content:
                    continue

                # Process and save results
                records = processor.process_results(content.splitlines())
                if records:
                    processor.save_results_externally(parent_job_id, file_name, records)
                    processor.save_results_internally(input_bucket_name, parent_job_id, file_name, records)
                    processor.update_job_status(parent_job_id, file_name)
            else:
                logger.error(f"No job found for {bedrock_job_short_id} in job status table.")

    except Exception as e:
        logger.error(f"Error in lambda handler: {e}")
        raise