import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';

import * as AwsCostForecast from '../lib/aws_cost_forecast-stack';
import { App } from 'aws-cdk-lib';

test('Empty Stack', () => {
  const app = new App();
  // WHEN
  const stack = new AwsCostForecast.AwsCostForecastStack(app, 'MyTestStack');
  // THEN
  expectCDK(stack).to(
    matchTemplate(
      {
        Resources: {},
      },
      MatchStyle.EXACT,
    ),
  );
});
