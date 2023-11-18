import * as cdk from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import * as TsImdbCdkPackage from '../lib/data_extraction/data_extraction_stack';

test('SQS Queue and SNS Topic Created', () => {
  const app = new cdk.App();
  // WHEN
  const stack = new TsImdbCdkPackage.DataExtractionStack(app, 'MyTestStack');
  // THEN

  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::SQS::Queue', {
    VisibilityTimeout: 300
  });
  template.resourceCountIs('AWS::SNS::Topic', 1);
});
