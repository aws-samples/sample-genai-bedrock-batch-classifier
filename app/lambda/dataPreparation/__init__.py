import logging
import os
from typing import Dict, Any
from utils.sqs_parser import extract_bucket_from_sqs_message
from utils.s3 import read_s3_file
from dataPreparation.dataProcessor import DataProcessor
from dataPreparation.environmentConfig import EnvironmentConfig

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


def lambda_handler(event: Dict, context: Any) -> Dict[str, Any]:
    """
    Process incoming S3 events and prepare data for Bedrock processing.

    Args:
        event (Dict[str, Any]): Lambda event
        context (LambdaContext): Lambda context

    """
    try:
        logger.info("Start data preparation processing.")

        # Initialize configuration
        config = EnvironmentConfig()
        processor = DataProcessor(config)

        for record in event["Records"]:
            # Extract bucket details
            input_bucket = extract_bucket_from_sqs_message(record["body"])
            input_bucket_name = input_bucket.get("input_bucket_name")

            if not input_bucket:
                continue

            input_key = input_bucket.get("input_key_name")
            file_extension = input_key.lower().split(".")[-1]
            file_content = read_s3_file(input_bucket_name, input_key)

            if not file_content:
                continue

            jsonl_content = processor.convert_to_jsonl(file_extension, file_content)
            
            if not jsonl_content:
                logger.warning(f"No valid content processed for file {input_key}")
                continue

            batches = processor.process_jsonl_batches(jsonl_content)
            
            if batches:
                processor.save_batches(batches)
                logger.info(f"Successfully processed {len(batches)} batches for {input_key}")

        return {
            "statusCode": 200,
            "body": "Processing completed successfully"
        }

    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error during processing: {str(e)}"
        }