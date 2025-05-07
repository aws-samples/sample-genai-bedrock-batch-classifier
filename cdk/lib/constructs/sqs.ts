import * as cdk from 'aws-cdk-lib';
import { Duration, RemovalPolicy, Tag } from 'aws-cdk-lib';
import { AccountPrincipal, Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

interface SqsProps {
  readonly name: string;
  readonly dlqName: string;
  readonly enforceSSL?: boolean;
  readonly removalPolicy?: RemovalPolicy;
  readonly deliveryDelay?: Duration;
  readonly visibilityTimeout?: Duration;
  readonly retentionPeriod?: Duration;
  readonly receiveMessageWaitTime?: Duration;
  readonly tags?: Tag;
}

export class SqsResource extends Construct {
  public readonly queue: Queue;

  constructor(scope: Construct, id: string, props: SqsProps) {
    super(scope, id);

    const {
      name,
      enforceSSL = true,
      removalPolicy = RemovalPolicy.DESTROY,
      deliveryDelay = Duration.seconds(3),
      visibilityTimeout = Duration.minutes(3),
      retentionPeriod = Duration.days(4),
      receiveMessageWaitTime = Duration.seconds(0),
    } = props;

    const deadLetterQueue = new Queue(this, props.dlqName, {
      queueName: props.dlqName,
      retentionPeriod: Duration.days(14),
      enforceSSL,
    });

    this.queue = new Queue(this, name, {
      queueName: name,
      deadLetterQueue: {
        maxReceiveCount: 3,
        queue: deadLetterQueue,
      },
      removalPolicy,
      enforceSSL,
      deliveryDelay,
      visibilityTimeout,
      retentionPeriod,
      receiveMessageWaitTime,
    });

    // Create a queue policy
    const queuePolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['sqs:SendMessage'],
      principals: [new AccountPrincipal(cdk.Stack.of(this).account)],
      resources: [this.queue.queueArn],
    });

    // Attach the queue policy to the queue
    this.queue.addToResourcePolicy(queuePolicy);
  }
}
