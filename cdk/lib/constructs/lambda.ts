import { Duration, RemovalPolicy, Tag } from 'aws-cdk-lib';
import { ISecurityGroup, IVpc, SubnetSelection } from 'aws-cdk-lib/aws-ec2';
import { ManagedPolicy, Role } from 'aws-cdk-lib/aws-iam';
import { Code, Function as func, ILayerVersion, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SnsDestination, SqsDestination } from 'aws-cdk-lib/aws-lambda-destinations';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import path = require('path');

interface LambdaProps {
  readonly name: string;
  readonly logGroupName: string;
  readonly logGroupRetention?: RetentionDays;
  readonly timeout?: Duration;
  readonly handler: string;
  readonly lambdaRole: Role;
  readonly runtime?: Runtime;
  readonly memorySize?: number | 512;
  readonly deadLetterQueueDestination?: Queue;
  readonly snsTopicDestination?: Topic;
  readonly environmentVariables?: { [key: string]: string };
  readonly removalPolicy?: RemovalPolicy;
  readonly vpc?: IVpc;
  readonly securityGroups?: ISecurityGroup[];
  readonly vpcSubnets?: SubnetSelection;
  readonly tags?: Tag;
  readonly layers?: ILayerVersion[];
}

const defaultLogGroupRetiontion = RetentionDays.TWO_YEARS;

export class LambdaResource extends Construct {
  public readonly lambdaFunction: func;

  constructor(scope: Construct, id: string, props: LambdaProps) {
    super(scope, id);

    const {
      name,
      logGroupName,
      handler,
      lambdaRole,
      memorySize,
      runtime = Runtime.PYTHON_3_11,
      timeout = Duration.seconds(60),
      logGroupRetention = defaultLogGroupRetiontion,
      deadLetterQueueDestination,
      snsTopicDestination,
      environmentVariables,
      removalPolicy = RemovalPolicy.DESTROY,
      vpc,
      securityGroups,
      vpcSubnets,
      layers,
    } = props;

    let lambdaDestination: SqsDestination | SnsDestination | undefined;
    if (deadLetterQueueDestination !== undefined) {
      lambdaDestination = new SqsDestination(deadLetterQueueDestination);
    } else if (snsTopicDestination !== undefined) {
      lambdaDestination = new SnsDestination(snsTopicDestination);
    }

    if(props.vpc) {
      lambdaRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'));
    }

    // Define a safe base path for Lambda code
    const projectRoot = path.resolve(__dirname, '../../../');
    const lambdaCodePath = path.join(__dirname, '../../../app/lambda');
    
    this.lambdaFunction = new func(this, name, {
      functionName: name,
      code: Code.fromAsset(lambdaCodePath),
      handler: handler,
      layers: layers,
      logGroup: new LogGroup(this, logGroupName, {
        retention: logGroupRetention,
        logGroupName: `/aws/lambda/${logGroupName}`,
        removalPolicy: removalPolicy,
      }),
      memorySize,
      timeout,
      runtime,
      role: lambdaRole,
      description: `Revision: ${new Date().toISOString()}`,
      onFailure: lambdaDestination,
      environment: environmentVariables,
      vpc,
      securityGroups,
      vpcSubnets,
    });
  }
}
