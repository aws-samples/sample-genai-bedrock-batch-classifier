import * as cdk from 'aws-cdk-lib';
import { Effect, PolicyDocument, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import * as nag from 'cdk-nag';
import { Construct } from 'constructs';
import { INTERNAL_PROCESSED_FOLDER, QUICKSIGHT_DATA_SCHEMA, QUICKSIGHT_PRINCIPAL_NAME, QUICKSIGHT_QUERY_MODE } from '../constants';
import { AthenaDatabaseResource } from '../constructs/athenadb';
import { GlueCrawlerResource } from '../constructs/glueCrawler';
import { IamRoleResource } from '../constructs/iam';
import { AthenaDataSourceResource } from '../constructs/quicksight/athenaDataSource';
import { QuicksightAnalysisResource } from '../constructs/quicksight/quicksightAnalysis';
import { QuicksightDashboardResource } from '../constructs/quicksight/quicksightDashboard';
import { QuicksightDataSetResource } from '../constructs/quicksight/quicksightDataSet';
import { QuicksightTableDefintionResource } from '../constructs/quicksight/quicksightTableDefinition';

interface AnalyticsStackProps {
  readonly env: cdk.Environment;
  readonly prefix: string;
  readonly postfix: string | undefined;
  readonly internalClassificationsBucketArn: string;
  readonly internalClassificationsBucketName: string;
  readonly athenaDatabaseName: string;
}

export class AnalyticsStack extends cdk.Stack {
  private readonly quicksightPrincipalArn: string;
  private readonly prefix: string;
  private readonly accountId: string;
  private readonly postfix: string | undefined;
  private readonly athenaDatabaseName: string;
  private readonly internalClassificationsBucketArn: string;
  private readonly internalClassificationsBucketName: string;

  constructor(scope: Construct, id: string, props: AnalyticsStackProps) { 
    super(scope, id, props);

    this.prefix = props.prefix;
    this.postfix = props.postfix;
    this.athenaDatabaseName = props.athenaDatabaseName;
    this.accountId = props.env.account as string;
    this.internalClassificationsBucketArn = props.internalClassificationsBucketArn;
    this.internalClassificationsBucketName = props.internalClassificationsBucketName;
    this.quicksightPrincipalArn = `arn:aws:quicksight:${props.env.region}:${this.accountId}:group/default/${QUICKSIGHT_PRINCIPAL_NAME}`;

    // Don’t forget to grant permission to the service role from LakeFormation to a new table.
    this.createQuicksightDashboard();

    // Create Glue Database
    this.createAthenaDatabase();
    this.createGlueCrawler();
  }

  createAthenaDatabase = () => {
    return new AthenaDatabaseResource(this, this.athenaDatabaseName, {
      databaseName: this.athenaDatabaseName,
      description: 'Database for analytics data'
    });
  }

  createGlueCrawler = () => {
    nag.NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'Resource wildcard is required for Glue crawler to access tables and partitions in the specified database',
      },
      {
        id: 'AwsSolutions-GL1',
        reason: 'Added security configuration with CloudWatch Logs encryption enabled'
      }
    ]);
    // Create IAM role for Glue Crawler
    const crawlerRoleName = 'glue-crawler-role';
    const crawlerRole = new IamRoleResource(this,
      crawlerRoleName, {
        name: `${this.prefix}-${crawlerRoleName}-${this.postfix}`,
        assumedByServicePrincipalName: 'glue.amazonaws.com',
        inlinePolicyDocument: new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                's3:GetObject', 
                's3:ListBucket'
              ],
              resources: [
                this.internalClassificationsBucketArn,
                `${this.internalClassificationsBucketArn}/*`,
              ],
              sid: 'S3Access',
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              resources: [
                `arn:aws:glue:${this.region}:${this.account}:catalog`,
                `arn:aws:glue:${this.region}:${this.account}:database/${this.athenaDatabaseName}`,
                `arn:aws:glue:${this.region}:${this.account}:table/${this.athenaDatabaseName}/*`
              ],
              actions: [
                'glue:GetDatabase',
                'glue:GetDatabases',
                'glue:CreateDatabase',
                'glue:GetTable',
                'glue:GetTables',
                'glue:CreateTable',
                'glue:UpdateTable',
                'glue:BatchCreatePartition',
                'glue:CreatePartition',
                'glue:GetPartition',
                'glue:GetPartitions',
                'glue:BatchGetPartition',
                'glue:UpdatePartition'
              ],
              sid: 'GlueAccess',
            })
          ]
        })
    }).iamRole;

    const crawlerName = 'glue-crawler';
    const crawler = new GlueCrawlerResource(this, crawlerName, {
      crawlerName: `${this.prefix}-${crawlerName}-${this.postfix}`,
      databaseName: this.athenaDatabaseName,
      roleArn: crawlerRole.roleArn,
      s3Targets: [{
        path: `${this.internalClassificationsBucketName}/${INTERNAL_PROCESSED_FOLDER}`,
      }],
      schedule: {
        cronExpression: 'cron(0/15 * * * ? *)'
      },
    });
  }

  createQuicksightDashboard = () => {
    const athenaDataSource = new AthenaDataSourceResource(
      this, 
      'athena-datasource', {
        dataSourceName: `${this.prefix}-datasource-${this.postfix}`,
        principalArn: this.quicksightPrincipalArn,
      }
    ).dataSource;

    const quicksightViewThemeArn = 'arn:aws:quicksight::aws:theme/MIDNIGHT';

    const quicksightDataSet = new QuicksightDataSetResource(
      this,
      'quicksight-dataset', {
        dataSetName: `${this.prefix}-dataset-${this.postfix}`,
        athenaDatabaseName: this.athenaDatabaseName,
        athenaTableName: INTERNAL_PROCESSED_FOLDER,
        dataSourceArn: athenaDataSource.attrArn,
        principalArn: this.quicksightPrincipalArn,
        queryMode: QUICKSIGHT_QUERY_MODE,
        data: QUICKSIGHT_DATA_SCHEMA,
      }
    ).dataSet;

    const quicksightDefinition = new QuicksightTableDefintionResource(
      this,
      'quicksight-table-definition', {
        sheetName: `Classifications Report`,
        tableVisualId: 'classifications-visualid',
        dataSet: quicksightDataSet,
        data: QUICKSIGHT_DATA_SCHEMA,
      }
    ).definition;

    const quicksightAnalysis = new QuicksightAnalysisResource(
      this,
      'quicksight-analysis', {
        analysisName: `${this.prefix}-classifications-analysis-${this.postfix}`,
        definition: quicksightDefinition,
        themeArn: quicksightViewThemeArn,
        principalArn: this.quicksightPrincipalArn,
      }
    ).analysis;

    const quicksightDashboard = new QuicksightDashboardResource(
      this,
      'quicksight-dashboard', {
        dashboardName: `${this.prefix}-classifications-dashboard-${this.postfix}`,
        definition: quicksightDefinition,
        themeArn: quicksightViewThemeArn,
        principalArn: this.quicksightPrincipalArn,
      }
    ).dashboard;
  }
}