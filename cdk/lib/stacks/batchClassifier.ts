import * as cdk from 'aws-cdk-lib';
import { Effect, PolicyDocument, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LayerVersion } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Bucket, EventType } from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import * as nag from 'cdk-nag';
import { Construct } from 'constructs';
import { BEDROCK_AGENT_MODEL, CLASSIFICATIONS_INPUT_FOLDER, CLASSIFICATIONS_OUTPUT_FOLDER, MAX_CONCURRENCY, PANDA_ACCOUNT } from '../constants';
import { IamRoleResource } from '../constructs/iam';
import { LambdaResource } from '../constructs/lambda';
import { SqsResource } from '../constructs/sqs';

interface BatchClassifierStackProps {
  readonly env: cdk.Environment;
  readonly prefix: string;
  readonly postfix: string | undefined;
  readonly internalClassificationsBucketArn: string;
  readonly jobProcessingStatusTable: string;
}

export class BatchClassifierStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: BatchClassifierStackProps) { 
    super(scope, id, props);

    const prefix = props.prefix;
    const postfix = props.postfix;

    const batchClassificationsQueueName = `batch-classifications-queue`;
    const batchClassificationsDlqName = `batch-classifications-dlq`;
    
    const batchClassificationsQueue = new SqsResource(
      this,
      batchClassificationsQueueName,
      {
        name: `${prefix}-${batchClassificationsQueueName}-${postfix}`,
        dlqName: `${prefix}-${batchClassificationsDlqName}-${postfix}`,
      }
    ).queue;

    const internalClassificationsBucket = Bucket.fromBucketArn(
      this,
      'classifications-bucket',
      props.internalClassificationsBucketArn,
    );

    if(internalClassificationsBucket) {
      internalClassificationsBucket.addEventNotification(
        EventType.OBJECT_CREATED,
        new SqsDestination(batchClassificationsQueue),
        {
          prefix: `${CLASSIFICATIONS_INPUT_FOLDER}/`,
          suffix: '.jsonl',
        },
      );
    }

    batchClassificationsQueue.addToResourcePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['SQS:SendMessage'],
        principals: [new ServicePrincipal('s3.amazonaws.com')],
        resources: [batchClassificationsQueue.queueArn],
        conditions: {
          ArnLike: {
            'aws:SourceArn': props.internalClassificationsBucketArn,
          },
        },
      }),
    );

    const bedrockRoleName = 'bedrock-role';
    const bedrockRole = new IamRoleResource(
      this,
      bedrockRoleName,
      {
        name: `${prefix}-${bedrockRoleName}-${postfix}`,
        assumedByServicePrincipalName: 'bedrock.amazonaws.com',
        inlinePolicyDocument: new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
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
                `arn:aws:bedrock:${props.env.region}:${props.env.account}:model-invocation-job/*`,
                `arn:aws:bedrock:${props.env.region}::foundation-model/${BEDROCK_AGENT_MODEL}`,
              ],
              actions: ['bedrock:*'],
              sid: 'BedrockAccess',
            }),
          ],
        }
      ),
    });

    const batchProcessingLambdaRoleName = 'batch-classifier-role';
    const batchProcessingLambdaRole = new IamRoleResource(
      this,
      batchProcessingLambdaRoleName,
      {
        name: `${prefix}-${batchProcessingLambdaRoleName}-${postfix}`,
        assumedByServicePrincipalName: 'lambda.amazonaws.com',
        inlinePolicyDocument: new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
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
                `arn:aws:bedrock:${props.env.region}:${props.env.account}:model-invocation-job/*`,
                `arn:aws:bedrock:${props.env.region}::foundation-model/${BEDROCK_AGENT_MODEL}`,
              ],
              actions: ['bedrock:*'],
              sid: 'BedrockAccess',
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                bedrockRole.iamRole.roleArn,
              ],
              actions: ['iam:PassRole'],
              sid: 'IAMAccess',
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
            })
          ],
        }
      ),
    });

    const pandasLayer = LayerVersion.fromLayerVersionArn(this, `${prefix}-pandas-layer-${postfix}`,
      `arn:aws:lambda:${props.env.region}:${PANDA_ACCOUNT}:layer:AWSSDKPandas-Python311:20`
    );

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

    nag.NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-L1',
        reason: 'Python 11 Version contain numpy library which is required here',
      },
    ]);
    const batchProcessingFunctionName = `batch-classifier-function`;
    const batchProcessingFunction = new LambdaResource(this,
      batchProcessingFunctionName,
      {
        name: `${prefix}-${batchProcessingFunctionName}-${postfix}`,
        logGroupName: `${prefix}-${batchProcessingFunctionName}-lg-${postfix}`,
        handler: 'batchClassifier.lambda_handler',
        lambdaRole: batchProcessingLambdaRole.iamRole,
        layers: [pandasLayer],
        environmentVariables: {
          BEDROCK_ROLE: bedrockRole.iamRole.roleArn,
          BEDROCK_MODEL_ID: BEDROCK_AGENT_MODEL,
          BEDROCK_JOB_PREFIX: `${prefix}-job`,
          OUTPUT_FOLDER_NAME: CLASSIFICATIONS_OUTPUT_FOLDER,
          JOB_STATUS_TABLE: props.jobProcessingStatusTable,
        },
      }
    ).lambdaFunction;

    batchProcessingFunction.addEventSource(
      new SqsEventSource(batchClassificationsQueue, {
        batchSize: 1,
        maxConcurrency: MAX_CONCURRENCY,
      }),
    );
  }
}