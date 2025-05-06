import os
import logging
from typing import Dict, List, Optional
from boto3 import client
import pandas as pd
import io
from io import BytesIO

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

s3_client = client("s3")

def save_file_to_s3(file_content: str, bucket_name: str, file_key: str) -> None:
    """
    Upload file to S3 bucket.

    Args:
        file_content (str): Content to be uploaded to S3
        bucket_name (str): Name of the S3 bucket
        file_key (str): Key (path) where the file will be stored in S3

    """
    try:
        if not file_key.endswith(".xlsx"):
            file_content = BytesIO(file_content.encode('utf-8'))

        s3_client.upload_fileobj(file_content, bucket_name, file_key)
        logger.info(f"File uploaded successfully to s3://{bucket_name}/{file_key}")
    except Exception as e:
        logger.error(f"Error saving file to S3: {e}")
        raise

def read_s3_xlsx_file(bucket: str, key: str) -> List[Dict]:
    """
    Read and parse Excel file from S3.

    Args:
        bucket (str): Name of the S3 bucket
        key (str): Key (path) of the Excel file in S3

    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = io.BytesIO(response['Body'].read())
        
        all_records = []
        df_dict = pd.read_excel(excel_data, sheet_name=None)
        
        logger.info(f"Found {len(df_dict)} sheets in Excel file")
        
        for sheet_name, df in df_dict.items():
            logger.info(f"Processing sheet: {sheet_name} with {len(df)} records")
            
            records = df.replace({pd.NA: None}).to_dict('records')
            cleaned_records = [
                {k: v for k, v in record.items() if pd.notna(v) and v is not None}
                for record in records
            ]
            
            all_records.extend([r for r in cleaned_records if r])
            logger.info(f"Added {len(cleaned_records)} cleaned records from sheet {sheet_name}")
        
        logger.info(f"Total records processed: {len(all_records)}")
        return all_records
        
    except Exception as e:
        logger.error(f"Error reading Excel file from S3: {e}")
        raise

def read_s3_file(bucket_name: str, file_key: str) -> Optional[str]:
    """
    Read file content from S3.

    Args:
        bucket_name (str): Name of the S3 bucket
        file_key (str): Key (path) of the file in S3

    """
    try:
        file_extension = file_key.lower().split('.')[-1]
        
        if file_extension in ["xlsx", "xls"]:
            return read_s3_xlsx_file(bucket_name, file_key)
        elif file_extension in ["csv", "json", "out"]:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            return response["Body"].read().decode('utf-8')
        else:
            logger.error(f"Unsupported file type: {file_extension}")
        
    except Exception as e:
        logger.error(f"Error reading S3 file: {e}")
        return None
