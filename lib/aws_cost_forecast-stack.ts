import { CfnDataset, CfnJob, CfnProject, CfnRecipe, CfnSchedule } from 'aws-cdk-lib/aws-databrew';
import { RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AccountPrincipal, CompositePrincipal, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { CfnReportDefinition } from 'aws-cdk-lib/aws-cur';
import { Bucket, BucketEncryption, CfnBucket } from 'aws-cdk-lib/aws-s3';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';
import { ForecastingProperties } from './forecast_properties';
import { AwsCustomResource, AwsCustomResourcePolicy } from 'aws-cdk-lib/custom-resources';

export class AwsCostForecastStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const dataBrewRole = new Role(this, 'costAndUsageReportRole', {
      roleName: ForecastingProperties.ROLE_ARN,
      assumedBy: new CompositePrincipal(
        new ServicePrincipal('databrew.amazonaws.com'),
        new ServicePrincipal('forecast.amazonaws.com'),
      ),
      path: '/service-role/',
    });
    const reportBucket = new Bucket(this, 'costAndUsageReportBucket', {
      encryption: BucketEncryption.S3_MANAGED,
      bucketName: ForecastingProperties.REPORT_BUCKET_NAME,
      versioned: true,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    reportBucket.addToResourcePolicy(
      new PolicyStatement({
        resources: [reportBucket.arnForObjects('*'), reportBucket.bucketArn],
        actions: ['s3:GetBucketAcl', 's3:GetBucketPolicy', 's3:PutObject', 's3:GetObject'],
        principals: [
          new ServicePrincipal('billingreports.amazonaws.com'),
          new ServicePrincipal('databrew.amazonaws.com'),
          new AccountPrincipal(this.account),
        ],
      }),
    );
    const prefixCreation = new BucketDeployment(this, 'PrefixCreator', {
      sources: [Source.asset('./assets')],
      destinationBucket: reportBucket,
      destinationKeyPrefix: `${ForecastingProperties.REPORT_DEFINITION.s3Prefix}`, // optional prefix in destination bucket
    });
    prefixCreation.node.addDependency(reportBucket);
    const outputBucket = new Bucket(this, 'outputBucket', {
      encryption: BucketEncryption.S3_MANAGED,
      bucketName: ForecastingProperties.FORECAST_OUTPUT_BUCKET_NAME,
      versioned: true,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    new CfnReportDefinition(this, 'costAndUsageReport', ForecastingProperties.REPORT_DEFINITION).addDependsOn(
      reportBucket.node.defaultChild as CfnBucket,
    );

    outputBucket.grantReadWrite(dataBrewRole);
    reportBucket.grantReadWrite(dataBrewRole);
    const cfnDataset = new CfnDataset(this, 'Dataset', {
      name: ForecastingProperties.DATASET_NAME,
      input: {
        s3InputDefinition: {
          bucket: reportBucket.bucketName,
          key: `${ForecastingProperties.REPORT_DEFINITION.s3Prefix}/<[^/]+>.parquet`,
        },
      },
      format: 'PARQUET',
    });

    const recipe = new CfnRecipe(this, 'dataBrewRecipe', {
      name: ForecastingProperties.GLUE_DATA_BREW_PROJECT.recipeName,
      steps: [
        {
          action: {
            operation: 'GROUP_BY',
            parameters: {
              groupByAggFunctionOptions:
                '[{"sourceColumnName":"line_item_unblended_cost","targetColumnName":"line_item_unblended_cost_sum","targetColumnDataType":"double","functionName":"SUM"}]',
              sourceColumns: '["line_item_usage_start_date","product_product_name","line_item_usage_account_id"]',
              useNewDataFrame: 'true',
            },
          },
        },
        {
          action: {
            operation: 'DATE_FORMAT',
            parameters: {
              dateTimeFormat: 'yyyy-mm-dd',
              functionStepType: 'DATE_FORMAT',
              sourceColumn: 'line_item_usage_start_date',
              targetColumn: 'line_item_usage_start_date_DATEFORMAT',
            },
          },
        },
        {
          action: {
            operation: 'DELETE',
            parameters: {
              sourceColumns: '["line_item_usage_start_date"]',
            },
          },
        },
      ],
    });

    recipe.node.addDependency(prefixCreation);
    const cfnProject = new CfnProject(this, 'dataBrewProject', ForecastingProperties.GLUE_DATA_BREW_PROJECT);
    cfnProject.addDependsOn(recipe);
    cfnProject.addDependsOn(cfnDataset);
    const publishRecipe = new AwsCustomResource(this, `publishRecipe`, {
      onUpdate: {
        service: 'DataBrew',
        action: 'publishRecipe',
        parameters: {
          Name: recipe.name,
        },
        physicalResourceId: { id: `publishRecipe` },
      },
      onDelete: {
        service: 'DataBrew',
        action: 'deleteRecipeVersion',
        parameters: {
          Name: `${recipe.name}` /* required */,
          RecipeVersion: '1.0',
        },
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE }),
    });
    publishRecipe.node.addDependency(recipe);
    const cfnJob = new CfnJob(this, 'dataBrewRecipeJob', {
      type: 'RECIPE',
      projectName: ForecastingProperties.GLUE_DATA_BREW_PROJECT.name,
      name: `${ForecastingProperties.PREFIX}-job`,
      outputs: [
        {
          //compressionFormat: "GZIP",
          format: 'CSV',
          location: {
            bucket: outputBucket.bucketName,
            key: `${ForecastingProperties.PREFIX}-output`,
          },
          overwrite: true,
        },
      ],
      roleArn: dataBrewRole.roleArn,
    });
    cfnJob.addDependsOn(cfnProject);
    new CfnSchedule(this, 'dataBrewJobSchedule', {
      cronExpression: 'Cron(0 23 * * ? *)',
      name: `${ForecastingProperties.PREFIX}-job-schedule`,
      jobNames: [`${ForecastingProperties.PREFIX}-job`],
    }).addDependsOn(cfnJob);

    const forecastDataset = new AwsCustomResource(this, `forecastDataset`, {
      onUpdate: {
        service: 'ForecastService',
        action: 'createDataset',
        parameters: {
          Domain: 'CUSTOM',
          DatasetName: 'amazonForecastDataset',
          DataFrequency: 'D',
          Schema: {
            Attributes: [
              {
                AttributeName: 'timestamp',
                AttributeType: 'timestamp',
              },
              {
                AttributeName: 'item_id',
                AttributeType: 'string',
              },
              {
                AttributeName: 'account_id',
                AttributeType: 'string',
              },
              {
                AttributeName: 'target_value',
                AttributeType: 'float',
              },
            ],
          },
          DatasetType: 'TARGET_TIME_SERIES',
        },
        physicalResourceId: { id: `forecastDataset` },
      },
      onDelete: {
        service: 'ForecastService',
        action: 'deleteDataset',
        parameters: {
          DatasetArn: `arn:aws:forecast:${this.region}:${this.account}:dataset/amazonForecastDataset`,
        },
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE }),
    });

    const forecastDatasetGroup = new AwsCustomResource(this, `forecastDatasetGroup`, {
      onUpdate: {
        service: 'ForecastService',
        action: 'createDatasetGroup',
        parameters: {
          DatasetGroupName: 'amazonForecastDatasetGroup',
          Domain: 'CUSTOM',
          DatasetArns: [`arn:aws:forecast:us-east-1:${this.account}:dataset/amazonForecastDataset`],
        },
        physicalResourceId: { id: `forecastDatasetGroup` },
      },
      onDelete: {
        service: 'ForecastService',
        action: 'deleteDatasetGroup',
        parameters: {
          DatasetGroupArn: `arn:aws:forecast:${this.region}:${this.account}:dataset-group/amazonForecastDatasetGroup` /* required */,
        },
      },
      policy: AwsCustomResourcePolicy.fromStatements([
        new PolicyStatement({
          actions: [
            'forecast:CreateDatasetGroup',
            'forecast:DeleteDatasetGroup',
            'logs:CreateLogGroup',
            'logs:CreateLogStream',
            'logs:PutLogEvents',
            'databrew:StartJobRun',
            'iam:PassRole',
          ],
          resources: ['*'],
        }),
      ]),
    });
    forecastDatasetGroup.node.addDependency(forecastDataset);
    const startDataBrewJob = new AwsCustomResource(this, `startDataBrewJob`, {
      onUpdate: {
        service: 'DataBrew',
        action: 'startJobRun',
        parameters: {
          Name: `${ForecastingProperties.PREFIX}-job`,
        },
        physicalResourceId: { id: `startDataBrewJob` },
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE }),
    });

    startDataBrewJob.node.addDependency(cfnJob);

    const datasetImportJob = new AwsCustomResource(this, `forecastDatasetImportJob`, {
      onUpdate: {
        service: 'ForecastService',
        action: 'createDatasetImportJob',
        parameters: {
          DataSource: {
            S3Config: {
              Path: `s3://${outputBucket.bucketName}/${ForecastingProperties.PREFIX}-output`,
              RoleArn: `${dataBrewRole.roleArn}`,
            },
          },
          DatasetImportJobName: 'amazonForecastDatasetImportJob',
          TimestampFormat: 'yyyy-MM-dd',
          DatasetArn: forecastDataset.getResponseField('DatasetArn'),
        },
        physicalResourceId: { id: `forecastDatasetImportJob` },
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE }),
    });
    datasetImportJob.node.addDependency(forecastDatasetGroup);

    new AwsCustomResource(this, `forecastPredictor`, {
      onUpdate: {
        service: 'ForecastService',
        action: 'createPredictor',
        parameters: {
          PredictorName: `costAndUsageReportTrainPredictor`,
          ForecastHorizon: 7,
          FeaturizationConfig: {
            ForecastFrequency: 'D',
            ForecastDimensions: ['account_id'],
          },
          PerformAutoML: true,
          InputDataConfig: {
            DatasetGroupArn: `arn:aws:forecast:${this.region}:${this.account}:dataset-group/amazonForecastDatasetGroup`,
          },
        },
        physicalResourceId: { id: `forecastPredictor` },
      },
      onDelete: {
        service: 'ForecastService',
        action: 'deletePredictor',
        parameters: {
          PredictorArn: `arn:aws:forecast:${this.region}:${this.account}:predictor/costAndUsageReportTrainPredictor`,
        },
      },
      policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE }),
    }).node.addDependency(forecastDatasetGroup);
  }
}
