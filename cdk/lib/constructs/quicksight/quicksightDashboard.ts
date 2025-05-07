import * as cdk from 'aws-cdk-lib';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';
import { Construct } from 'constructs';
import path = require('path');

interface QuicksightDashboardResourceProps {
  readonly dashboardName: string;
  readonly definition: any;
  readonly themeArn: string;
  readonly principalArn: string;
}

export class QuicksightDashboardResource extends Construct {
  public readonly dashboard: quicksight.CfnDashboard;

  constructor(scope: Construct, id: string, props: QuicksightDashboardResourceProps) {
    super(scope, id);

    this.dashboard = this.createQuicksightDashboard(
      props.dashboardName,
      props.definition,
      props.themeArn,
      props.principalArn,
    );
  }

  private createQuicksightDashboard = (
    dashboardName: string,
    definition: any,
    themeArn: string,
    principalArn: string,
  ) => {
    return new quicksight.CfnDashboard(this, dashboardName, {
      awsAccountId: cdk.Stack.of(this).account,
      dashboardId: dashboardName,
      name: dashboardName,
      themeArn,
      definition,
      dashboardPublishOptions: {
        adHocFilteringOption: {
          availabilityStatus: 'ENABLED',
        },
        exportToCsvOption: {
          availabilityStatus: 'ENABLED',
        },
        sheetControlsOption: {
          visibilityState: 'EXPANDED',
        },
      },
      permissions: [
        {
          principal: principalArn,
          actions: [
            'quicksight:DescribeDashboard',
            'quicksight:ListDashboardVersions',
            'quicksight:UpdateDashboardPermissions',
            'quicksight:QueryDashboard',
            'quicksight:UpdateDashboard',
            'quicksight:DeleteDashboard',
            'quicksight:DescribeDashboardPermissions',
            'quicksight:UpdateDashboardPublishedVersion',
          ],
        },
      ],
    });
  }
}
