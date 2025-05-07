import json
import logging
from batchClassifier.environmentConfig import EnvironmentConfig
from batchClassifier.dataProcessor import DataProcessor
from utils.sqs_parser import extract_bucket_from_sqs_message
import os
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


def lambda_handler(event: Dict, context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function that processes SQS messages containing S3 event information
    and creates Bedrock batch inference jobs for JSONL files.
    
    Args:
        event (dict): The AWS Lambda event object containing SQS records
        context (object): The AWS Lambda context object

    """
    try:
        logger.info("Start data classification processing.")

        # Initialize configuration
        config = EnvironmentConfig()
        processor = DataProcessor(config)

        output_folder_name = config.get("output_folder_name")

        for record in event["Records"]:
            logger.info(f"Processing record: {json.dumps(record)}")
            
            bucket = extract_bucket_from_sqs_message(record["body"])

            if not bucket:
                logger.error("Failed to extract bucket information")
                continue

            input_bucket_name = bucket.get("input_bucket_name")
            input_key_name = bucket.get("input_key_name")
            base_filename = input_key_name.split("/")[-1].split(".", 1)[0]

            if not input_key_name.endswith(".jsonl"):
                logger.warning(f"Skipping non-JSONL file: {input_key_name}")
                continue

            input_path = f"s3://{input_bucket_name}/{input_key_name}"
            output_path = f"s3://{input_bucket_name}/{output_folder_name}/"
            
            processor.create_claude_batch_inference_job(
                input_path,
                output_path,
                base_filename,
            )

        return {
            "statusCode": 200,
            "body": "Processing completed successfully"
        }

    except Exception as e:
        error_msg = f"Error processing event: {str(e)}"
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "body": error_msg
        }