import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import { IPrincipal, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface AthenaDatabaseResourceProps {
  readonly databaseName: string;
  readonly description?: string;
  readonly locationUri?: string;
  readonly parameters?: { [key: string]: string };
  readonly tags?: { [key: string]: string };
}

export class AthenaDatabaseResource extends Construct {
  public readonly database: glue.CfnDatabase;
  public readonly databaseName: string;

  constructor(scope: Construct, id: string, props: AthenaDatabaseResourceProps) {
    super(scope, id);

    this.databaseName = props.databaseName;

    // Create the database input configuration
    const databaseInput: glue.CfnDatabase.DatabaseInputProperty = {
      name: props.databaseName,
      description: props.description || `Database ${props.databaseName} created for Athena queries`,
      parameters: props.parameters || {},
      locationUri: props.locationUri || undefined,
    };

    // Create the Glue Database
    this.database = new glue.CfnDatabase(this, 'Database', {
      catalogId: cdk.Stack.of(this).account,
      databaseInput
    });

    // Add tags if provided
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        cdk.Tags.of(this.database).add(key, value);
      });
    }

    // Add default tags
    cdk.Tags.of(this.database).add('CreatedBy', 'CDK');
    cdk.Tags.of(this.database).add('Service', 'Athena');

    this.grantRead(new cdk.aws_iam.ServicePrincipal('quicksight.amazonaws.com'));
    this.grantWrite(new cdk.aws_iam.ServicePrincipal('quicksight.amazonaws.com'));
  }

  /**
   * Grants read permissions on the database to the given principal
   */
  private grantRead(principal: IPrincipal): void {
    principal.addToPrincipalPolicy(new PolicyStatement({
      actions: [
        'glue:GetDatabase',
        'glue:GetDatabases',
        'glue:GetTable',
        'glue:GetTables',
        'glue:GetPartition',
        'glue:GetPartitions',
        'glue:BatchGetPartition'
      ],
      resources: [
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${this.databaseName}`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${this.databaseName}/*`
      ]
    }));
  }

  /**
   * Grants write permissions on the database to the given principal
   */
  private grantWrite(principal: IPrincipal): void {
    principal.addToPrincipalPolicy(new PolicyStatement({
      actions: [
        'glue:CreateTable',
        'glue:DeleteTable',
        'glue:BatchDeleteTable',
        'glue:UpdateTable',
        'glue:CreatePartition',
        'glue:BatchCreatePartition',
        'glue:DeletePartition',
        'glue:BatchDeletePartition',
        'glue:UpdatePartition'
      ],
      resources: [
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${this.databaseName}`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${this.databaseName}/*`
      ]
    }));
  }
}
