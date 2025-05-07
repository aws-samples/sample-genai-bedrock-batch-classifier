import json
import os
import logging
from typing import Dict, Any, List, Optional
from csv import DictReader
from typing import Dict, List, Optional
from utils.dynamodb import create_job_status_record
from utils.id_generator import generate_random_id, get_current_date_short_str
from utils.s3 import save_file_to_s3
from dataPreparation.environmentConfig import EnvironmentConfig

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class DataProcessor:
    """Handles data processing and conversion operations."""

    def __init__(self, config: EnvironmentConfig):
        """
        Initialize DataProcessor.

        Args:
            config (EnvironmentConfig): Environment configuration

        """
        self.config = config

    def convert_to_jsonl(self, file_extension: str, file_content: str) -> Optional[str]:
        """
        Convert file content to JSONL format.

        Args:
            file_extension (str): File extension
            file_content (str): Content to convert

        """
        try:
            records = self._parse_content(file_extension, file_content)
            if not records:
                return None

            return self._convert_records_to_jsonl(records)

        except Exception as e:
            logger.error(f"Error converting to JSONL: {str(e)}")
            return None

    def process_jsonl_batches(
        self, 
        jsonl_content: str
    ) -> List[List[str]]:
        """
        Process JSONL content into batches.

        Args:
            jsonl_content (str): JSONL content to process

        """
        try:
            batch_size = self.config.get_int("batch_size", 10)
            lines = [line for line in jsonl_content.splitlines() if line.strip()]
            total_records = len(lines)
            logger.info(f"Processing {total_records} records")

            if not self._validate_batch_size(total_records, batch_size):
                return []

            return self._create_batches(lines, batch_size)

        except Exception as e:
            logger.error(f"Error processing JSONL batches: {str(e)}")
            raise

    @staticmethod
    def _parse_content(file_extension: str, content: str) -> Optional[List[Dict]]:
        """
        Parse content based on file type.

        Internal method to parse different file formats into a common dictionary format.

        Args:
            file_extension: File extension indicating format (csv, json, xlsx, xls)
            content

        """
        try:
            if file_extension == "csv":
                return list(DictReader(content.splitlines()))
            elif file_extension == "json":
                return json.loads(content)
            elif file_extension in ["xlsx", "xls"]:
                return content
            else:
                logger.error(f"Unsupported file type: {file_extension}")
                return None
        except Exception as e:
            logger.error(f"Error parsing content: {str(e)}")
            return None

    def _convert_records_to_jsonl(self, records: List[Dict]) -> Optional[str]:
        """
        Convert records to JSONL format.

        Internal method to convert parsed records into JSONL format with proper structure
        for Bedrock processing.

        Args:
            records: List of dictionaries containing record data

        """
        try:
            jsonl_lines = []
            text_field = self.config.get("input_mapping_text_field")
            
            for record in records:
                try:
                    text_content = record.pop(text_field)
                    record_id = self._get_record_id(record)
                    
                    jsonl_record = {
                        "recordId": record_id,
                        "modelInput": self._create_model_input(text_content)
                    }
                    
                    jsonl_lines.append(json.dumps(jsonl_record, ensure_ascii=False))
                    
                except KeyError:
                    logger.warning(f"Missing text field {text_field} in record")
                    continue

            if not jsonl_lines:
                logger.warning("No valid records to convert")
                return None

            logger.info(f"Converted {len(jsonl_lines)} records to JSONL")
            return "\n".join(jsonl_lines)

        except Exception as e:
            logger.error(f"Error converting to JSONL: {str(e)}")
            return None

    def _get_record_id(self, record: Dict[str, str]) -> str:
        """
        Extract record ID with BOM handling.

        Internal method to get record ID from input data, handling BOM characters
        and generating random IDs if needed.

        Args:
            record: Dictionary containing record data

        """
        id_field = self.config.get("input_mapping_id_field")
        bom_id_field = f"\ufeff{id_field}"
        return record.get(id_field) or record.get(bom_id_field) or generate_random_id()

    def _create_model_input(self, text_content: str) -> Dict[str, Any]:
        """
        Create model input structure for Bedrock.

        Internal method to format text content into the required structure for
        Bedrock model input.

        Args:
            text_content: Text content to process

        """
        return {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 2048,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": text_content
                }]
            }],
            "system": self.config.get("prompt")
        }

    def _validate_batch_size(self, total_records: int, batch_size: int) -> bool:
        """
        Validate batch size configuration.

        Internal method to ensure batch size meets minimum requirements.

        Args:
            total_records: Total number of records to process
            batch_size: Configured batch size

        """
        
        minimum_records = int(self.config.get("minimum_records_per_batch", 10))
        
        if total_records < minimum_records:
            logger.warning(
                f"Total records ({total_records}) is less than minimum required ({minimum_records})"
            )
            return False
            
        if batch_size < minimum_records:
            logger.warning(
                f"Batch size ({batch_size}) is less than minimum required ({minimum_records})"
            )
            return False
            
        return True

    def _create_batches(self, lines: List[str], batch_size: int) -> List[List[str]]:
        """
        Create batches from content lines.

        Internal method to split content into appropriate sized batches.

        Args:
            lines: List of JSONL lines to batch
            batch_size: Size of each batch

        """
        batches = []
        current_batch = []
        minimum_records = int(self.config.get("minimum_records_per_batch", 10))

        for line in lines:
            current_batch.append(line)
            
            if len(current_batch) >= batch_size:
                batches.append(current_batch)
                current_batch = []

        if current_batch:
            if len(current_batch) >= minimum_records:
                batches.append(current_batch)
            elif batches:
                batches[-1].extend(current_batch)

        return batches
    
    def save_batches(
        self,
        batches: List[List[str]]
    ) -> None:
        """
        Save processed batches to S3.

        Args:
            batches (List[List[str]]): Processed batches

        """
        try:
            output_bucket = self.config.get("output_bucket_name")
            output_folder = self.config.get("output_folder_name")
            parent_id = generate_random_id()

            for i, batch in enumerate(batches):
                batch_content = "\n".join(batch)

                file_id = f"{parent_id}-batch{i+1}"
                current_date = get_current_date_short_str()
                base_filename = f"{output_folder}/{current_date}/{parent_id}/{file_id}.jsonl"
                
                save_file_to_s3(
                    batch_content,
                    output_bucket,
                    base_filename
                )

                create_job_status_record(
                    self.config.get("job_status_table"),
                    file_id,
                    "DRAFT"
                )

        except Exception as e:
            logger.error(f"Error saving batches: {str(e)}")
            raise