import * as cdk from 'aws-cdk-lib';
import { Effect, PolicyDocument, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LayerVersion } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Bucket, EventType } from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import * as nag from 'cdk-nag';
import { Construct } from 'constructs';
import { CLASSIFICATIONS_OUTPUT_FOLDER, INTERNAL_PROCESSED_FOLDER, MAX_CONCURRENCY, OUTPUT_FORMAT, PANDA_ACCOUNT } from '../constants';
import { IamRoleResource } from '../constructs/iam';
import { LambdaResource } from '../constructs/lambda';
import { SqsResource } from '../constructs/sqs';

interface BatchResultsProcessingStackProps {
  readonly env: cdk.Environment;
  readonly prefix: string;
  readonly postfix: string | undefined;
  readonly internalClassificationsBucketArn: string;
  readonly customerRequestsBucketArn: string;
  readonly jobProcessingStatusTable: string;
}

export class BatchResultsProcessingStack extends cdk.Stack { 
  constructor(scope: Construct, id: string, props: BatchResultsProcessingStackProps) { 
    super(scope, id, props);

    const prefix = props.prefix;
    const postfix = props.postfix;

    const featureName = 'batch-results-processing';

    const batchResultsProcessingQueueName = 'batch-results-queue';
    const batchResultsProcessingDlqName = 'batch-results-dlq';
    const batchResultsProcessingQueue = new SqsResource(
      this,
      batchResultsProcessingQueueName,
      {
        name: `${prefix}-${batchResultsProcessingQueueName}-${postfix}`,
        dlqName: `${prefix}-${batchResultsProcessingDlqName}-${postfix}`,
      }
    ).queue;

    batchResultsProcessingQueue.addToResourcePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['SQS:SendMessage'],
        principals: [new ServicePrincipal('s3.amazonaws.com')],
        resources: [batchResultsProcessingQueue.queueArn],
        conditions: {
          ArnLike: {
            'aws:SourceArn': props.internalClassificationsBucketArn,
          },
        },
      }),
    );

    const internalClassificationsBucket = Bucket.fromBucketArn(
      this,
      'internal-classifications-bucket',
      props.internalClassificationsBucketArn,
    );

    if(internalClassificationsBucket) {
      internalClassificationsBucket.addEventNotification(
        EventType.OBJECT_CREATED,
        new SqsDestination(batchResultsProcessingQueue),
        {
          prefix: `${CLASSIFICATIONS_OUTPUT_FOLDER}/`,
          suffix: '.jsonl.out',
        },
      );
    }

    nag.NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'Limited to specific DynamoDB and S3 actions required for batch processing functionality'
      },
      {
        id: 'AwsSolutions-IAM4',
        reason: 'Suppressing the AWS managed policy warning as we are adding explicit permissions for CloudWatch Logs',
      }
    ]);
    const batchResultsProcessingFunctionName = `${featureName}-function`;
    const batchResultsProcessingLambdaRoleName = `${featureName}-role`;
    const batchResultsProcessingLambdaRole = new IamRoleResource(
      this,
      batchResultsProcessingLambdaRoleName,
      {
        name:  `${prefix}-${batchResultsProcessingLambdaRoleName}-${postfix}`,
        assumedByServicePrincipalName: 'lambda.amazonaws.com',
        inlinePolicyDocument: new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                props.internalClassificationsBucketArn,
                `${props.internalClassificationsBucketArn}/*`,
                props.customerRequestsBucketArn,
                `${props.customerRequestsBucketArn}/*`,
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
                `arn:aws:logs:${props.env.region}:${props.env.account}:log-group:/aws/lambda/${prefix}-${batchResultsProcessingFunctionName}-lg-${postfix}:*`
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
                `arn:aws:logs:${props.env.region}:${props.env.account}:log-group:/aws/lambda/${prefix}-${batchResultsProcessingFunctionName}-lg-${postfix}`
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
    const batchResultsProcessingFunction = new LambdaResource(this,
      batchResultsProcessingFunctionName,
      {
        name: `${prefix}-${batchResultsProcessingFunctionName}-${postfix}`,
        logGroupName: `${prefix}-${batchResultsProcessingFunctionName}-lg-${postfix}`,
        handler: 'batchResultsProcessing.lambda_handler',
        lambdaRole: batchResultsProcessingLambdaRole.iamRole,
        layers: [pandasLayer],
        environmentVariables: {
          OUTPUT_BUCKET_ARN: props.customerRequestsBucketArn,
          OUTPUT_FOLDER_NAME: CLASSIFICATIONS_OUTPUT_FOLDER,
          JOB_STATUS_TABLE: props.jobProcessingStatusTable,
          OUTPUT_FORMAT,
          INTERNAL_PROCESSED_FOLDER,
        },
      }
    ).lambdaFunction;

    batchResultsProcessingFunction.addEventSource(
      new SqsEventSource(batchResultsProcessingQueue, {
        batchSize: 1,
        maxConcurrency: MAX_CONCURRENCY,
      }),
    );
  }
}