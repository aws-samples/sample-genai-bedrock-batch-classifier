import { TRAVEL_PROMPT } from './prompts/travel';
import { OUTPUT_FORMATS, QUICKSIGHT_QUERY_MODES } from './types';

// The constants below can be configured as needed
export const PREFIX = 'genai';
export const BEDROCK_AGENT_MODEL = 'anthropic.claude-3-5-haiku-20241022-v1:0';
export const BATCH_SIZE = 200; // minimum should be 100

export const CLASSIFICATIONS_INPUT_FOLDER = 'input_data';
export const CLASSIFICATIONS_OUTPUT_FOLDER = 'output_data';
export const OUTPUT_FORMAT = OUTPUT_FORMATS.CSV;

export const INTERNAL_PROCESSED_FOLDER = 'processed_data';

export const INPUT_MAPPING = {
  record_id: 'conversation_id',
  record_text: 'conversation',
}

export const ATHENA_DATABASE_NAME = 'genai-classifications';

 // a principal group who will have access to QuickSight Resources
export const QUICKSIGHT_PRINCIPAL_NAME = 'quicksight-access';
export const QUICKSIGHT_QUERY_MODE = QUICKSIGHT_QUERY_MODES.DIRECT_QUERY;
export const QUICKSIGHT_DATA_SCHEMA = [{
  name: 'id',
  type: 'STRING',
  label: 'ID',
  width: '257px',
}, {
  name: 'input_text',
  type: 'STRING',
  label: 'Initial Text',
  width: '428px',
}, {
  name: 'class',
  type: 'STRING',
  label: 'Classification',
  isFilterable: true,
  width: '205px',
}, {
  name: 'rationale',
  type: 'STRING',
  label: 'Rationale',
  width: '428px',
}, {
  name: 'partition_0',
  type: 'STRING',
  label: 'Date',
}];

export const PROMPT = TRAVEL_PROMPT;
export const S3_ACCESS_LOGGING_BUCKET_RETENTON_DAYS = 90;