import os
import logging
from typing import Dict, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

class EnvironmentConfig:
    """Manages environment configuration settings."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        self.config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from environment variables."""
        try:
            required_vars = [
                "OUTPUT_BUCKET_ARN",
                "OUTPUT_FOLDER_NAME",
                "INPUT_MAPPING_TEXT_FIELD",
                "INPUT_MAPPING_ID_FIELD",
                "PROMPT",
                "BATCH_SIZE",
                "MINIMUM_RECORDS_PER_BATCH",
                "JOB_STATUS_TABLE"
            ]

            for var in required_vars:
                value = os.environ.get(var)
                if not value:
                    raise ValueError(f"Missing required environment variable: {var}")
                self.config[var.lower()] = value.strip()

            self._validate_bucket_arn()
            self._process_field_names()
            logger.info("Environment configuration loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def _validate_bucket_arn(self) -> None:
        """Validate S3 bucket ARN and extract bucket name."""
        bucket_arn = self.config.get("output_bucket_arn", "")
        if not bucket_arn.startswith("arn:aws:s3:::"):
            raise ValueError(
                f"Invalid S3 bucket ARN format. Expected 'arn:aws:s3:::bucket-name', "
                f"got {bucket_arn}"
            )
        
        self.config["output_bucket_name"] = bucket_arn.replace("arn:aws:s3:::", "")

    def _process_field_names(self) -> None:
        """Process field names and handle BOM characters."""
        for field in ["input_mapping_text_field", "input_mapping_id_field"]:
            if field in self.config:
                self.config[field] = self.config[field].replace("\ufeff", "")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value for a key.

        Args:
            key (str): Configuration key
            default (Any): Default value if key not found
        """
        try:
            return self.config.get(key.lower(), default)
        except Exception as e:
            logger.warning(f"Error retrieving config value for {key}: {str(e)}")
            return default

    def get_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        """
        Get integer configuration value.

        Args:
            key (str): Configuration key
            default (Optional[int]): Default value if key not found
        """
        try:
            value = self.get(key, default)
            return int(str(value)) if value is not None else default
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value for {key}")
            return default
