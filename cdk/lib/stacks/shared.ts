import * as cdk from 'aws-cdk-lib';
import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, TableEncryption } from 'aws-cdk-lib/aws-dynamodb';
import { AnyPrincipal, Effect, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { BlockPublicAccess, BucketPolicy } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { S3_ACCESS_LOGGING_BUCKET_RETENTON_DAYS } from '../constants/mutable';
import { BucketResource } from '../constructs/bucket';
import { DynamoDBResource } from '../constructs/dynamodb';

interface SharedStackProps {
  readonly env: cdk.Environment;
  readonly prefix: string;
  readonly postfix: string | undefined;
}

export class SharedStack extends cdk.Stack {
  public readonly internalClassificationsBucketArn: string; 
  public readonly customerRequestsBucketArn: string;
  public readonly jobProcessingStatusTable: string;
  public readonly internalClassificationsBucketName: string;
  public readonly serverAccessLogsBucket: cdk.aws_s3.Bucket;

  constructor(scope: Construct, id: string, props: SharedStackProps) { 
    super(scope, id, props);

    const prefix = props.prefix;
    const postfix = props.postfix;

    const serverAccessLogsBucketName = 'server-access-logs';
    this.serverAccessLogsBucket = new BucketResource(
      this,
      serverAccessLogsBucketName,
      {
        name: `${prefix}-${serverAccessLogsBucketName}-${postfix}`,
        versioned: false,
        removalPolicy: RemovalPolicy.RETAIN,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        lifecycleRules: {
          expiration: cdk.Duration.days(S3_ACCESS_LOGGING_BUCKET_RETENTON_DAYS),
        },
      },
    ).bucket;

    const serverAccessLogsBucketPolicy = new BucketPolicy(this,
      `${prefix}-server-access-logs-bucketpolicy-${postfix}`, {
        bucket: this.serverAccessLogsBucket,
      }
    );
    serverAccessLogsBucketPolicy.document.addStatements(
      new PolicyStatement({
        actions: ['s3:PutObject'],
        effect: Effect.ALLOW,
        principals: [new ServicePrincipal('logging.s3.amazonaws.com')],
        resources: [this.serverAccessLogsBucket.bucketArn, `${this.serverAccessLogsBucket.bucketArn}/*`],
      }),
    );
    
    // Add a deny statement for non-SSL requests to comply with AwsSolutions-S10
    serverAccessLogsBucketPolicy.document.addStatements(
      new PolicyStatement({
        sid: 'DenyNonSSLRequests',
        actions: ['s3:*'],
        effect: Effect.DENY,
        principals: [new AnyPrincipal()],
        resources: [this.serverAccessLogsBucket.bucketArn, `${this.serverAccessLogsBucket.bucketArn}/*`],
        conditions: {
          'Bool': {
            'aws:SecureTransport': 'false'
          }
        }
      }),
    );

    const internalClassificationsBucketName = 'internal-classifications-bucket';
    const internalClassificationsBucket = new BucketResource(
      this,
      internalClassificationsBucketName,
      {
        name: `${prefix}-${internalClassificationsBucketName}-${postfix}`,
        versioned: true,
        removalPolicy: RemovalPolicy.RETAIN,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        serverAccessLogsBucket: this.serverAccessLogsBucket,
      },
    ).bucket;

    const customerRequestsBucketName = 'customer-requests-bucket';
    const customerRequestsBucket = new BucketResource(
      this,
      customerRequestsBucketName,
      {
        name: `${prefix}-${customerRequestsBucketName}-${postfix}`,
        versioned: true,
        removalPolicy: RemovalPolicy.RETAIN,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        serverAccessLogsBucket: this.serverAccessLogsBucket,
      },
    ).bucket;

    const jobProcessingStatusName = 'batch-processing-status';
    const jobProcessingStatusTable = new DynamoDBResource(this, jobProcessingStatusName, {
      name: `${prefix}-${jobProcessingStatusName}-${postfix}`,
      partitionKey: {
        name: 'id',
        type: AttributeType.STRING,
      },
      encryption: TableEncryption.AWS_MANAGED,
    }).table;

    this.internalClassificationsBucketArn = internalClassificationsBucket.bucketArn;
    this.internalClassificationsBucketName = internalClassificationsBucket.bucketName;
    this.customerRequestsBucketArn = customerRequestsBucket.bucketArn;
    this.jobProcessingStatusTable = jobProcessingStatusTable.tableName;
  }
}