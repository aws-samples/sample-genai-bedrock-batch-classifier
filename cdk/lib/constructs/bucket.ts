import { RemovalPolicy } from 'aws-cdk-lib';
import { IKey } from 'aws-cdk-lib/aws-kms';
import { BlockPublicAccess, Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface BucketProps {
  readonly name: string;
  readonly versioned?: boolean | true;
  readonly removalPolicy?: RemovalPolicy | RemovalPolicy.DESTROY;
  readonly lifecycleRules?: any;
  readonly blockPublicAccess?: BlockPublicAccess;
  readonly encryption?: BucketEncryption | BucketEncryption.S3_MANAGED;
  readonly encryptionKey?: IKey;
  readonly enforceSSL?: boolean | true;
  readonly serverAccessLogsBucket?: Bucket;
}

export class BucketResource extends Construct {
  public readonly bucket: Bucket;

  constructor(scope: Construct, id: string, props: BucketProps) {
    super(scope, id);

    const s3BucketName = props.name.toLowerCase().replace('_', '-');

    if (props.encryption == BucketEncryption.S3_MANAGED) {
      this.bucket = new Bucket(this, s3BucketName, {
        bucketName: s3BucketName,
        encryption: props.encryption,
        enforceSSL: props.enforceSSL,
        versioned: props.versioned,
        removalPolicy: props.removalPolicy,
        serverAccessLogsBucket: props.serverAccessLogsBucket,
        serverAccessLogsPrefix: props.serverAccessLogsBucket ? `access-logs/${s3BucketName}/` : undefined,
      });
    } else {
      this.bucket = new Bucket(this, s3BucketName, {
        bucketName: s3BucketName,
        encryption: props.encryption,
        encryptionKey: props.encryptionKey,
        enforceSSL: props.enforceSSL,
        versioned: props.versioned,
        removalPolicy: props.removalPolicy,
        serverAccessLogsBucket: props.serverAccessLogsBucket,
        serverAccessLogsPrefix: props.serverAccessLogsBucket ? `access-logs/${s3BucketName}/` : undefined,
      });
    }
    if (props.lifecycleRules) {
      this.bucket.addLifecycleRule(props.lifecycleRules);
    }
  }
}
