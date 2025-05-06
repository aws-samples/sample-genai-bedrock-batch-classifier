import json
import os
import logging
from typing import Dict, Optional

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

def extract_bucket_from_sqs_message(message: str) -> Optional[Dict[str, str]]:
    """
    Extract S3 bucket info from SQS message.

    Args:
        message (str): SQS message containing S3 event information

    """
    try:
        message_body = json.loads(message)
        record = message_body["Records"][0]
        s3_record = record["s3"]
        
        input_bucket_name = s3_record["bucket"]["name"]
        input_key_name = s3_record["object"]["key"]
        
        logger.info(f"Extracted bucket: {input_bucket_name}, key: {input_key_name}")
        
        return {
            "input_bucket_name": input_bucket_name,
            "input_key_name": input_key_name
        }
    except Exception as e:
        logger.error(f'Error extracting bucket from SQS message: {e}')
        return None
