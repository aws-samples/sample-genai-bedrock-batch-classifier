import os
import logging
from utils.dynamodb import update_or_create_job_status_record
from batchClassifier.environmentConfig import EnvironmentConfig
from boto3 import client

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class DataProcessor:
    """Handles data processing and AWS service interactions."""

    def __init__(self, config: EnvironmentConfig):
        """
        Initialize DataProcessor with AWS clients.

        Args:
            config (EnvironmentConfig): Environment configuration
            bedrock_client: Boto3 Bedrock client

        """
        self.bedrock_client = client("bedrock")
        self.config = config

    def create_claude_batch_inference_job(
        self,
        input_data_s3_uri: str,
        output_data_s3_uri: str,
        base_filename: str,
    ) -> None:
        """
        Creates a Bedrock batch inference job.
        
        Args:
            input_data_s3_uri: Input data S3 URI
            output_data_s3_uri: Output data S3 URI
            base_filename: Batch job name

        """
        try:
            bedrock_job_prefix = self.config.get("bedrock_job_prefix")
            role_arn = self.config.get("bedrock_role")
            model_id = self.config.get("bedrock_model_id")

            input_data_config = {
                "s3InputDataConfig": {
                    "s3Uri": input_data_s3_uri
                }
            }

            output_data_config = {
                "s3OutputDataConfig": {
                    "s3Uri": output_data_s3_uri
                }
            }

            job_name = f"{bedrock_job_prefix}-{base_filename}"

            bedrock_job = self.bedrock_client.create_model_invocation_job(
                roleArn=role_arn,
                modelId=model_id,
                jobName=job_name,
                inputDataConfig=input_data_config,
                outputDataConfig=output_data_config
            )
            bedrock_job_full_id = bedrock_job.get("jobArn")
            logger.info(f"Batch Inference Job {job_name} created successfully with {bedrock_job_full_id}")

            update_or_create_job_status_record(
                self.config.get("job_status_table"),
                base_filename,
                {
                    "job_status": "RUNNING",
                    "bedrock_job_full_id": bedrock_job_full_id,
                    "bedrock_job_short_id": bedrock_job_full_id.split("/")[-1]
                }
            )

        except Exception as e:
            logger.error(f"Error creating batch inference job: {str(e)}")
            raise