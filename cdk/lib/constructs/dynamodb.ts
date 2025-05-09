import { Attribute, Table, TableEncryption } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';

interface DynamoDBProps {
  readonly name: string;
  readonly partitionKey: Attribute;
  readonly encryption?: TableEncryption | TableEncryption.AWS_MANAGED;
  readonly pointInTimeRecovery?: boolean;
}

export class DynamoDBResource extends Construct {
  public readonly table: Table;

  constructor(scope: Construct, id: string, props: DynamoDBProps) {
    super(scope, id);

    this.table = new Table(this, props.name, {
      tableName: props.name,
      partitionKey: props.partitionKey,
      pointInTimeRecovery: props.pointInTimeRecovery,
    });
  }
}
