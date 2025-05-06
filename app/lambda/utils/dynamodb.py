import datetime
import os
import logging
from typing import Any, Dict, List, Optional, Tuple
from utils.id_generator import get_current_date_full_str
from boto3 import client

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

dynamodb_client = client("dynamodb")

def get_dynamodb_value(value: Any) -> Dict[str, Any]:
    """
    Convert Python value to DynamoDB format.

    Args:
        value: Value to convert

    Returns:
        Dict[str, Any]: DynamoDB formatted value
    """
    if isinstance(value, str):
        return {"S": value}
    elif isinstance(value, bool):
        return {"BOOL": value}
    elif isinstance(value, (int, float)):
        return {"N": str(value)}
    elif isinstance(value, datetime):
        return {"S": value.isoformat()}
    elif isinstance(value, list):
        return {"L": [get_dynamodb_value(item) for item in value]}
    elif isinstance(value, dict):
        return {"M": {k: get_dynamodb_value(v) for k, v in value.items()}}
    elif value is None:
        return {"NULL": True}
    else:
        raise ValueError(f"Unsupported type for DynamoDB: {type(value)}")

def construct_filter_expression(filters: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, Any]], Dict[str, str]]:
    """
    Construct DynamoDB filter expression dynamically.

    Args:
        filters (Dict[str, Any]): Dictionary of field names and values to filter on

    """
    update_parts = []
    attr_values: Dict[str, Dict[str, Any]] = {}
    attr_names: Dict[str, str] = {}

    for key, value in filters.items():
        # Create placeholders for attribute name and value
        name_placeholder = f"#{key}"
        value_placeholder = f":{key}"
        
        # Build update expression parts
        update_parts.append(f"{name_placeholder} = {value_placeholder}")

        # Build attribute names dictionary
        attr_names[name_placeholder] = key

        # Build attribute values dictionary
        attr_values[value_placeholder] = get_dynamodb_value(value)
    
    # Combine all update parts with SET
    filter_expression = " ".join(update_parts)
    
    return filter_expression, attr_values, attr_names

def construct_update_expression(updates: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, Any]], Dict[str, str]]:
    """
    Construct DynamoDB update expression dynamically.

    Args:
        updates (Dict[str, Any]): Dictionary of field names and values to update

    """
    update_parts = []
    attr_values: Dict[str, Dict[str, Any]] = {}
    attr_names: Dict[str, str] = {}

    # Add current date to updates
    current_date = get_current_date_full_str()
    updates["last_updated_date"] = current_date

    for key, value in updates.items():
        # Create placeholders for attribute name and value
        name_placeholder = f"#{key}"
        value_placeholder = f":{key}"
        
        # Build update expression parts
        update_parts.append(f"{name_placeholder} = {value_placeholder}")

        # Build attribute names dictionary
        attr_names[name_placeholder] = key

        # Build attribute values dictionary
        attr_values[value_placeholder] = get_dynamodb_value(value)
    
    # Combine all update parts with SET
    update_expression = "SET " + ", ".join(update_parts)
    
    return update_expression, attr_values, attr_names

def get_job_status_record(table_name: str, item_id: str) -> Optional[Dict]:
    """
    Read item from DynamoDB.

    Args:
        table_name (str): Name of the DynamoDB table
        item_id (str): ID of the item to retrieve

    """
    try:
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={"id": {"S": item_id}},
        )
        return response
    except Exception as e:
        logger.error(f"Error reading from DynamoDB: {e}")
        return None

def create_job_status_record(table_name: str, item_id: str, job_status: str) -> None:
    """
    Write item to DynamoDB.

    Args:
        table_name (str): Name of the DynamoDB table
        item_id (str): ID of the item to create
        job_status (str): Status of the job
    """
    try:
        current_date = get_current_date_full_str()
        parent_id = item_id.partition("-batch")[0]

        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "id": {"S": item_id},
                "parent_id": {"S": parent_id},
                "created_date": {"S": current_date},
                "job_status": {"S": job_status},
            }
        )
        logger.info(f"Successfully created job status item to DynamoDB table {table_name} with id {item_id}")
    except Exception as e:
        logger.error(f"Error creating job status record in DynamoDB table: {e}")
        raise

def update_job_status_record(
    table_name: str,
    item_id: str,
    update_expression: str,
    attr_values: Dict[str, Any],
    attr_names: Dict[str, str]
) -> None:
    """
    Update item in DynamoDB.

    Args:
        table_name (str): Name of the DynamoDB table
        item_id (str): ID of the item to update
        update_expression (str): DynamoDB update expression
        attr_values (Dict[str, Any]): Expression attribute values
        attr_names (Dict[str, str]): Expression attribute names
    """
    try:
        update_params = {
            "TableName": table_name,
            "Key": {
                "id": {"S": item_id}
            },
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": attr_values,
            "ExpressionAttributeNames": attr_names
        }

        logger.info(f"Updating item {item_id} with parameters: {update_params}")
        dynamodb_client.update_item(**update_params)
        logger.info(f"Successfully updated job status record in DynamoDB table {table_name} with id {item_id}")
    except Exception as e:
        logger.error(f"Error updating job status record in DynamoDB table: {e}")
        raise

def update_or_create_job_status_record(
        table_name: str,
        item_id: str,
        updates: Dict[str, Any]
) -> None:
    """
    Create or update DynamoDB record for the given job status.

    Args:
        table_name (str): Name of the DynamoDB table
        item_id (str): ID of the item to update or create
        updates (Dict[str, Any]): Dictionary of fields to update
    """
    try:
        # Check if the item exists
        response = get_job_status_record(table_name, item_id)

        if "Item" in response:
            update_expr, attr_values, attr_names = construct_update_expression(updates)
            logger.info(f"Job status record is found.")
            update_job_status_record(
                table_name,
                item_id,
                update_expr,
                attr_values,
                attr_names
            )
        else:
            logger.info("Job status record is not found, creating a new one...")
            create_job_status_record(
                table_name,
                item_id,
                updates.get("job_status", "DRAFT")
            )

    except Exception as e:
        raise e

def get_job_status_items(
    table_name: str,
    filters: Dict[str, Any],
    page_size: Optional[int] = None,
    consistent_read: bool = False
) -> Optional[List[Dict]]:
    """
    Read items from DynamoDB with specific job status using pagination.

    Args:
        table_name (str): Name of the DynamoDB table
        filters (Dict[str, Any]): Filters applied to search the records
        page_size (Optional[int]): Number of items per page
        consistent_read (bool): Whether to use strongly consistent reads

    """
    try:
        items = []
        last_evaluated_key = None
        total_scanned = 0

        while True:
            # Prepare scan parameters
            filter_expr, attr_values, attr_names = construct_filter_expression(filters)
            scan_params = {
                "TableName": table_name,
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": attr_values,
                "ExpressionAttributeNames": attr_names,
                "ConsistentRead": consistent_read
            }

            # Add Limit if page_size is specified
            if page_size:
                scan_params["Limit"] = page_size

            # Add ExclusiveStartKey for pagination
            if last_evaluated_key:
                scan_params["ExclusiveStartKey"] = last_evaluated_key

            # Perform scan
            response = dynamodb_client.scan(**scan_params)

            # Add items from current page
            current_page_items = response.get("Items", [])
            items.extend(current_page_items)
            total_scanned += response.get("ScannedCount", 0)

            # Log progress
            logger.info(
                f"Retrieved {len(current_page_items)} items. "
                f"Total items: {len(items)}. "
                f"ScannedCount: {total_scanned}. "
                f"LastEvaluatedKey present: {response.get('LastEvaluatedKey') is not None}"
            )

            # Get the last evaluated key for next page
            last_evaluated_key = response.get("LastEvaluatedKey")

            # Break if no more pages
            if not last_evaluated_key:
                break

        logger.info(
            f"Scan completed. "
            f"Total items retrieved: {len(items)}. "
            f"Total items scanned: {total_scanned}"
        )
        return items

    except Exception as e:
        logger.error(f"Error reading from DynamoDB: {e}")
        return None