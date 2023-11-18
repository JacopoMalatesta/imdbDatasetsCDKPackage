import { Duration, Stack, type StackProps, RemovalPolicy } from 'aws-cdk-lib'
import * as s3 from 'aws-cdk-lib/aws-s3'
import * as lambda from 'aws-cdk-lib/aws-lambda'
import * as sns from 'aws-cdk-lib/aws-sns'
import * as s3_notifications from 'aws-cdk-lib/aws-s3-notifications'
import { type Construct } from 'constructs'

export class DataExtractionStack extends Stack {
  private readonly s3Bucket: s3.IBucket
  private readonly lambdaFunction: lambda.IFunction
  public readonly snsTopic: sns.ITopic

  constructor (scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)

    this.s3Bucket = this.createS3Bucket()
    this.lambdaFunction = this.createContainerizedLambda()
    this.s3Bucket.grantWrite(this.lambdaFunction)
    this.snsTopic = this.createSnsTopic()
    this.s3Bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED_PUT,
      new s3_notifications.SnsDestination(this.snsTopic)
    )
  }

  private createS3Bucket (): s3.Bucket {
    return new s3.Bucket(this, 'imdb-raw-zone', {
      bucketName: 'imdb-dataset-raw-zone',
      eventBridgeEnabled: true,
      removalPolicy: RemovalPolicy.RETAIN
    })
  }

  private createContainerizedLambda (): lambda.DockerImageFunction {
    return new lambda.DockerImageFunction(this, 'data-extraction-lambda',
      {
        functionName: 'imdb-datasets-data-extraction-lambda',
        code: lambda.DockerImageCode.fromImageAsset(__dirname,
          {
            file: 'Dockerfile'
          }),
        timeout: Duration.minutes(5),
        memorySize: 1024
      }
    )
  }

  private createSnsTopic (): sns.Topic {
    return new sns.Topic(this, 'data-extraction-topic', {
      topicName: 'imdb-data-extraction-topic'
    })
  }
}
