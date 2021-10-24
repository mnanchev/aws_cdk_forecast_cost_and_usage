import { CfnReportDefinitionProps } from 'aws-cdk-lib/aws-cur';
import { CfnProjectProps } from 'aws-cdk-lib/aws-databrew';

export class ForecastingProperties {
  static readonly PREFIX = 'cost-and-usage-report';
  static readonly REPORT_BUCKET_NAME = `${ForecastingProperties.PREFIX}-2021-12-12`;
  static readonly FORECAST_OUTPUT_BUCKET_NAME = `${ForecastingProperties.PREFIX}-forecasting-output-2021-12-12`;
  static readonly ROLE_ARN = 'dataBrewServiceRole';
  static readonly DATASET_NAME = `${ForecastingProperties.PREFIX}-dataset`;
  static readonly REPORT_DEFINITION: CfnReportDefinitionProps = {
    compression: 'Parquet',
    format: 'Parquet',
    refreshClosedReports: true,
    reportName: ForecastingProperties.REPORT_BUCKET_NAME,
    reportVersioning: 'CREATE_NEW_REPORT',
    s3Bucket: ForecastingProperties.REPORT_BUCKET_NAME,
    s3Prefix: '2021',
    s3Region: 'us-east-1',
    timeUnit: 'HOURLY',
  };
  static readonly GLUE_DATA_BREW_PROJECT: CfnProjectProps = {
    datasetName: ForecastingProperties.DATASET_NAME,
    name: `${ForecastingProperties.PREFIX}-forecasting-project`,
    recipeName: `${ForecastingProperties.PREFIX}-recipe`,
    roleArn: `arn:aws:iam::559706524079:role/service-role/${ForecastingProperties.ROLE_ARN}`,
  };
}
