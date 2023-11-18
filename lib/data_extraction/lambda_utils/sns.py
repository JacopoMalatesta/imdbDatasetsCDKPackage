import lambda_utils.credentials as credentials

import boto3
import botocore


def publish_message_to_sns_topic(
    *, topic_arn: str, message: str, region: str = "us-east-1"
) -> None:
    aws_credentials = credentials.get_aws_credentials()
    sns_client = boto3.client(
        "sns",
        aws_access_key_id=aws_credentials["aws_access_key"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=region,
    )
    try:
        sns_client.publish(TopicArn=topic_arn, Message=message)
    except botocore.exceptions.ClientError:
        print(f"Failed to publish {message=} to {topic_arn=} due to permission issues")
    except Exception:
        print(f"Failed to publish {message=} to {topic_arn=}")
    else:
        print(f"Successfully published {message=} to {topic_arn=}")
