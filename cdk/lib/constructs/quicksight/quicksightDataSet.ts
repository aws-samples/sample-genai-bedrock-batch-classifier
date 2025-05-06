import * as cdk from 'aws-cdk-lib';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';
import { Construct } from 'constructs';
import path = require('path');

interface QuicksightDataSetResourceProps {
  readonly dataSetName: string;
  readonly athenaDatabaseName: string;
  readonly athenaTableName: string;
  readonly dataSourceArn: string;
  readonly principalArn: string;
  readonly queryMode: string;
  readonly data: {
    name: string;
    type: string;
  }[];
}

export class QuicksightDataSetResource extends Construct {
  public readonly dataSet: quicksight.CfnDataSet;

  constructor(scope: Construct, id: string, props: QuicksightDataSetResourceProps) {
    super(scope, id);

    this.dataSet = this.createQuicksightDataSet(
      props.dataSetName,
      props.athenaDatabaseName,
      props.athenaTableName,
      props.data,
      props.dataSourceArn,
      props.principalArn,
      props.queryMode,
    );
  }

  private createQuicksightDataSet = (
    dataSetName: string,
    athenDatabaseName: string,
    athenaTableName: string,
    data: { name: string; type: string }[],
    dataSourceArn: string,
    principalArn: string,
    queryMode: string,
  ) => {
    return new quicksight.CfnDataSet(this, dataSetName, {
      name: dataSetName,
      dataSetId: dataSetName,
      awsAccountId: cdk.Stack.of(this).account,
      importMode: queryMode,
      permissions: [
        {
          actions: [
            'quicksight:DescribeDataSet',
            'quicksight:DescribeDataSetPermissions',
            'quicksight:PassDataSet',
            'quicksight:DescribeIngestion',
            'quicksight:ListIngestions',
            'quicksight:UpdateDataSet',
            'quicksight:DeleteDataSet',
            'quicksight:CreateIngestion',
            'quicksight:CancelIngestion',
            'quicksight:UpdateDataSetPermissions',
          ],
          principal: principalArn,
        },
      ],
      physicalTableMap: {
        quicksightTable: {
          relationalTable: {
            dataSourceArn,
            name: athenaTableName,
            schema: athenDatabaseName,
            inputColumns: data,
          },
        },
      },
      logicalTableMap: {
        quicksightTable: {
          alias: 'Quicksight Table',
          source: {
            physicalTableId: 'quicksightTable',
          },
        },
      },
    });
  };
}
