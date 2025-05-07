import * as cdk from 'aws-cdk-lib';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';
import { Construct } from 'constructs';
import path = require('path');

interface AthenaDataSourceProps {
  readonly dataSourceName: string;
  readonly principalArn: string;
}

export class AthenaDataSourceResource extends Construct {
  public readonly dataSource: quicksight.CfnDataSource;

  constructor(scope: Construct, id: string, props: AthenaDataSourceProps) {
    super(scope, id);

    this.dataSource = this.createQuicksightDataSource(
      props.dataSourceName,
      props.principalArn,
    );
  }

  private createQuicksightDataSource = (
    dataSourceName: string,
    principalArn: string,
  ) => {
    return new quicksight.CfnDataSource(this, dataSourceName, {
      name: dataSourceName,
      type: 'ATHENA',
      awsAccountId: cdk.Stack.of(this).account,
      dataSourceId: dataSourceName,
      dataSourceParameters: {
        athenaParameters: {
          workGroup: 'primary',
        },
      },
      permissions: [
        {
          actions: [
            'quicksight:DescribeDataSource',
            'quicksight:DescribeDataSourcePermissions',
            'quicksight:PassDataSource',
            'quicksight:UpdateDataSource',
            'quicksight:DeleteDataSource',
            'quicksight:UpdateDataSourcePermissions',
          ],
          principal: principalArn,
        },
      ],
    });
  };
}
