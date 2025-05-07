import * as cdk from 'aws-cdk-lib';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';
import { Construct } from 'constructs';

interface QuicksightAnalysisResourceProps {
  readonly analysisName: string;
  readonly definition: any;
  readonly themeArn: string;
  readonly principalArn: string;
}

export class QuicksightAnalysisResource extends Construct {
  public readonly analysis: quicksight.CfnAnalysis;

  constructor(scope: Construct, id: string, props: QuicksightAnalysisResourceProps) {
    super(scope, id);

    this.analysis = this.createQuicksightAnalysis(
      props.analysisName,
      props.definition,
      props.themeArn,
      props.principalArn,
    );
  }

  private createQuicksightAnalysis = (
    analysisName: string,
    definition: any,
    themeArn: string,
    principalArn: string,
  ) => {
    return new quicksight.CfnAnalysis(this, analysisName, {
      analysisId: analysisName,
      name: analysisName,
      awsAccountId: cdk.Stack.of(this).account,
      themeArn,
      definition,
      permissions: [
        {
          actions: [
            'quicksight:RestoreAnalysis',
            'quicksight:UpdateAnalysisPermissions',
            'quicksight:DeleteAnalysis',
            'quicksight:QueryAnalysis',
            'quicksight:DescribeAnalysisPermissions',
            'quicksight:DescribeAnalysis',
            'quicksight:UpdateAnalysis',
          ],
          principal: principalArn,
        },
      ],
    });
  };
}
