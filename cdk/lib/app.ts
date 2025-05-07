#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
import { ATHENA_DATABASE_NAME, PREFIX } from './constants';
import { AnalyticsStack } from './stacks/analytics';
import { BatchClassifierStack } from './stacks/batchClassifier';
import { BatchResultsProcessingStack } from './stacks/batchResultsProcessing';
import { DataPreparationStack } from './stacks/dataPreparation';
import { SharedStack } from './stacks/shared';


const POSTFIX = process.env.CDK_DEFAULT_REGION as string; // can be ACCOUNT_ID and REGION and STAGE to ensure unique naming across environments

const app = new cdk.App();
const environmentVariables = { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION }

// Create the shared resources first
const sharedStack = new SharedStack(app, 'SharedStack', {
  env: environmentVariables,
  prefix: PREFIX,
  postfix: POSTFIX,
});

// Create the batch classifier stack
const batchClassifierStack = new BatchClassifierStack(
  app,
  'BatchClassifierStack',
  {
    env: environmentVariables,
    prefix: PREFIX,
    postfix: POSTFIX,
    internalClassificationsBucketArn: sharedStack.internalClassificationsBucketArn,
    jobProcessingStatusTable: sharedStack.jobProcessingStatusTable,
  }
);

// Create the data preparation stack
const dataPreparationStack = new DataPreparationStack(
  app,
  'DataPreparationStack',
  {
    env: environmentVariables,
    prefix: PREFIX,
    postfix: POSTFIX,
    internalClassificationsBucketArn: sharedStack.internalClassificationsBucketArn,
    customerRequestsBucketArn: sharedStack.customerRequestsBucketArn,
    jobProcessingStatusTable: sharedStack.jobProcessingStatusTable,
  }
);

// Create the batch results processing stack
const batchResultsProcessingStack = new BatchResultsProcessingStack(
  app,
  'BatchResultsProcessingStack',
  {
    env: environmentVariables,
    prefix: PREFIX,
    postfix: POSTFIX,
    internalClassificationsBucketArn: sharedStack.internalClassificationsBucketArn,
    customerRequestsBucketArn: sharedStack.customerRequestsBucketArn,
    jobProcessingStatusTable: sharedStack.jobProcessingStatusTable,
  }
);

// Create the analytics stack that visualizes data through QuickSight Dashboad
const analyticsStack = new AnalyticsStack(
  app,
  'AnalyticsStack',
  {
    env: environmentVariables,
    prefix: PREFIX,
    postfix: POSTFIX,
    athenaDatabaseName: ATHENA_DATABASE_NAME,
    internalClassificationsBucketArn: sharedStack.internalClassificationsBucketArn,
    internalClassificationsBucketName: sharedStack.internalClassificationsBucketName,
  }
);

// cdk nag configuration
const deploymentStacks = [
  sharedStack,
  batchClassifierStack,
  dataPreparationStack,
  batchResultsProcessingStack,
  analyticsStack,
];

deploymentStacks.forEach((stack) => {
  Aspects.of(stack).add(
    new AwsSolutionsChecks({
      verbose: true,
    }),
  );
});