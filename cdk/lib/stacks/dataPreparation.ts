import * as cdk from 'aws-cdk-lib';
import { Effect, PolicyDocument, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LayerVersion } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Bucket, EventType } from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import * as nag from 'cdk-nag';
import { Construct } from 'constructs';
import { BATCH_SIZE, CLASSIFICATIONS_INPUT_FOLDER, INPUT_MAPPING, MAX_CONCURRENCY, MINIMUM_RECORDS_PER_BATCH, PANDA_ACCOUNT, PROMPT } from '../constants';
import { IamRoleResource } from '../constructs/iam';
import { LambdaResource } from '../constructs/lambda';
import { SqsResource } from '../constructs/sqs';

interface DataPreparationStackProps {
  readonly env: cdk.Environment;
  readonly prefix: string;
  readonly postfix: string | undefined;
  readonly internalClassificationsBucketArn: string;
  readonly customerRequestsBucketArn: string;
  readonly jobProcessingStatusTable: string;
}

export class DataPreparationStack extends cdk.Stack { 
  constructor(scope: Construct, id: string, props: DataPreparationStackProps) { 
    super(scope, id, props);

    const prefix = props.prefix;
    const postfix = props.postfix;

    const featureName = 'data-preparation';

    const customerRequestsQueueName = 'customer-requests-queue';
    const customerRequestsDlqName = 'customer-requests-dlq';
    const customerRequestsQueue = new SqsResource(
      this,
      customerRequestsQueueName,
      {
        name: `${prefix}-${customerRequestsQueueName}-${postfix}`,
        dlqName: `${prefix}-${customerRequestsDlqName}-${postfix}`,
      }
    ).queue;

    customerRequestsQueue.addToResourcePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['SQS:SendMessage'],
        principals: [new ServicePrincipal('s3.amazonaws.com')],
        resources: [customerRequestsQueue.queueArn],
        conditions: {
          ArnLike: {
            'aws:SourceArn': props.customerRequestsBucketArn,
          },
        },
      }),
    );

    const customerRequestsBucket = Bucket.fromBucketArn(
      this,
      'customer-requests-bucket',
      props.customerRequestsBucketArn,
    );

    if(customerRequestsBucket) {
      customerRequestsBucket.addEventNotification(
        EventType.OBJECT_CREATED,
        new SqsDestination(customerRequestsQueue),
        {
          prefix: `${CLASSIFICATIONS_INPUT_FOLDER}/`,
        },
      );  
    }

    // Add CDK-NAG suppressions for IAM findings
    nag.NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-IAM4',
        reason: 'Suppressing the AWS managed policy warning as we are adding explicit permissions for CloudWatch Logs',
      },
      {
        id: 'AwsSolutions-IAM5',
        reason: 'Wildcard resource is required for logs:CreateLogGroup as the log group needs to be created before it can be referenced',
      }
    ]);
    
    const dataPreparationFunctionName = `${featureName}-function`;
    const dataPreparationLambdaRoleName = `${featureName}-role`;
    const dataPreparationLambdaRole = new IamRoleResource(
      this,
      dataPreparationLambdaRoleName,
      {
        name:  `${prefix}-${dataPreparationLambdaRoleName}-${postfix}`,
        assumedByServicePrincipalName: 'lambda.amazonaws.com',
        inlinePolicyDocument: new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                props.customerRequestsBucketArn,
                `${props.customerRequestsBucketArn}/*`,
                props.internalClassificationsBucketArn,
                `${props.internalClassificationsBucketArn}/*`,
              ],
              actions: [
                's3:GetObject',
                's3:PutObject',
                's3:ListBucket',
                's3:DeleteObject'
              ],
              sid: 'S3Access',
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                `arn:aws:dynamodb:${props.env.region}:${props.env.account}:table/${props.jobProcessingStatusTable}`
              ],
              actions: [
                'dynamodb:GetItem',
                'dynamodb:PutItem',
                'dynamodb:UpdateItem',
                'dynamodb:Query',
                'dynamodb:Scan'
              ],
              sid: 'DynamoDBAccess',
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                `arn:aws:logs:${props.env.region}:${props.env.account}:log-group:/aws/lambda/${prefix}-${dataPreparationFunctionName}-lg-${postfix}:*`
              ],
              actions: [
                'logs:CreateLogStream',
                'logs:PutLogEvents'
              ],
              sid: 'CloudWatchLogsAccess',
            }),
            // Add permission to create the log group as well
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                `arn:aws:logs:${props.env.region}:${props.env.account}:log-group:/aws/lambda/${prefix}-${dataPreparationFunctionName}-lg-${postfix}`
              ],
              actions: [
                'logs:CreateLogGroup'
              ],
              sid: 'CloudWatchLogsGroupAccess',
            }),
          ],
        }
      ),
    });

    const pandasLayer = LayerVersion.fromLayerVersionArn(this, `${prefix}-pandas-layer-${postfix}`,
      `arn:aws:lambda:${props.env.region}:${PANDA_ACCOUNT}:layer:AWSSDKPandas-Python311:20`
    );

    nag.NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-L1',
        reason: 'Python 11 Version contain numpy library which is required here',
      },
    ]);
    const dataPreparationFunction = new LambdaResource(this,
      dataPreparationFunctionName,
      {
        name: `${prefix}-${dataPreparationFunctionName}-${postfix}`,
        logGroupName: `${prefix}-${dataPreparationFunctionName}-lg-${postfix}`,
        handler: 'dataPreparation.lambda_handler',
        lambdaRole: dataPreparationLambdaRole.iamRole,
        layers: [pandasLayer],
        environmentVariables: {
          OUTPUT_BUCKET_ARN: props.internalClassificationsBucketArn,
          OUTPUT_FOLDER_NAME: CLASSIFICATIONS_INPUT_FOLDER,
          INPUT_MAPPING_TEXT_FIELD: INPUT_MAPPING.record_text,
          INPUT_MAPPING_ID_FIELD: INPUT_MAPPING.record_id,
          BATCH_SIZE: `${BATCH_SIZE}`,
          MINIMUM_RECORDS_PER_BATCH: `${MINIMUM_RECORDS_PER_BATCH}`,
          JOB_STATUS_TABLE: props.jobProcessingStatusTable,
          PROMPT,
        },
      }
    ).lambdaFunction;

    dataPreparationFunction.addEventSource(
      new SqsEventSource(customerRequestsQueue, {
        batchSize: 1,
        maxConcurrency: MAX_CONCURRENCY,
      }),
    );
  }
}