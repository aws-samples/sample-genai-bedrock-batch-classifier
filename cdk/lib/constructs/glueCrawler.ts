import * as cdk from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface S3Target {
  path: string;
  exclusions?: string[];
  connectionName?: string;
}

export interface JdbcTarget {
  path: string;
  connectionName: string;
  exclusions?: string[];
}

export interface CrawlerSchedule {
  cronExpression: string;
}

export interface CrawlerConfiguration {
  Version?: number;
  Grouping?: {
    TableGroupingPolicy?: 'CombineCompatibleSchemas' | 'NONE';
  };
  CsvOptions?: {
    delimiter?: string;
    header?: boolean;
    comments?: string;
    quoteSymbol?: string;
  };
  Parameters?: { [key: string]: string };
}

export interface GlueCrawlerResourceProps {
  readonly crawlerName: string;
  readonly databaseName: string;
  readonly s3Targets?: S3Target[];
  readonly jdbcTargets?: JdbcTarget[];
  readonly schedule?: CrawlerSchedule;
  readonly tablePrefix?: string;
  readonly configuration?: CrawlerConfiguration;
  readonly roleArn: string;
  readonly tags?: { [key: string]: string };
  readonly schemaChangePolicy?: {
    deleteBehavior?: 'LOG' | 'DELETE_FROM_DATABASE' | 'DEPRECATE_IN_DATABASE';
    updateBehavior?: 'LOG' | 'UPDATE_IN_DATABASE';
  };
  readonly recrawlPolicy?: {
    recrawlBehavior: 'CRAWL_EVERYTHING' | 'CRAWL_NEW_FOLDERS_ONLY';
  };
}

export class GlueCrawlerResource extends Construct {
  public readonly crawler: glue.CfnCrawler;
  public readonly role: iam.Role;

  constructor(scope: Construct, id: string, props: GlueCrawlerResourceProps) {
    super(scope, id);

    // Create or use provided IAM role
    const roleArn = props.roleArn;

    // Prepare crawler configuration
    const configuration: CrawlerConfiguration = {
      Version: props.configuration?.Version ?? 1.0,
      Grouping: props.configuration?.Grouping ?? {
        TableGroupingPolicy: 'CombineCompatibleSchemas'
      },
      ...props.configuration
    };

    // Create the crawler
    this.crawler = new glue.CfnCrawler(this, 'Crawler', {
      name: props.crawlerName,
      role: roleArn,
      databaseName: props.databaseName,
      targets: {
        s3Targets: props.s3Targets,
        jdbcTargets: props.jdbcTargets
      },
      schedule: props.schedule ? {
        scheduleExpression: props.schedule.cronExpression
      } : undefined,
      tablePrefix: props.tablePrefix,
      configuration: JSON.stringify(configuration),
      schemaChangePolicy: props.schemaChangePolicy ?? {
        deleteBehavior: 'LOG',
        updateBehavior: 'UPDATE_IN_DATABASE'
      },
      recrawlPolicy: props.recrawlPolicy ?? {
        recrawlBehavior: 'CRAWL_EVERYTHING'
      },
    });

    // Add tags
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        cdk.Tags.of(this.crawler).add(key, value);
      });
    }
  }
}
