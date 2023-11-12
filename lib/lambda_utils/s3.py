import io
import logging
from typing import Dict
from abc import ABC, abstractmethod
import lambda_utils.credentials as credentials
import lambda_utils.logger as logging_utils

import boto3
import botocore


logger: logging.Logger = logging_utils.get_logger(module=__name__)


class S3ClientInterface(ABC):
    @abstractmethod
    def upload_file_to_s3(self, *, byte_file: bytes, s3_key: str) -> None:
        """"""

    @abstractmethod
    def download_file_from_s3(self, *, s3_key: str) -> io.BytesIO:
        """"""


class S3Client(S3ClientInterface):
    def __init__(self, *, bucket_name: str, aws_region: str = "eu-west-1") -> None:
        self.bucket_name = bucket_name
        self.aws_region = aws_region

        aws_credentials: Dict[str, str] = credentials.get_aws_credentials()

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_credentials["aws_access_key"],
            aws_secret_access_key=aws_credentials["aws_secret_access_key"],
            region_name=aws_region,
        )

    def upload_file_to_s3(self, *, byte_file: bytes, s3_key: str) -> None:
        full_path: str = f"S3://{self.bucket_name}/{s3_key}"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=s3_key, Body=byte_file
            )
        except botocore.exceptions.ClientError:
            logger.error(
                f"Failed to upload file to {full_path} due to permission issues"
            )
            raise
        except Exception:
            logger.error(f"Failed to upload file to {full_path}")
            raise
        else:
            logger.info(f"Successfully wrote file to {full_path}")

    def download_file_from_s3(self, *, s3_key: str) -> io.BytesIO:
        buffer = io.BytesIO()
        full_path: str = f"S3://{self.bucket_name}/{s3_key}"

        try:
            self.s3_client.download_fileobj(
                Bucket=self.bucket_name, Key=s3_key, Fileobj=buffer
            )
        except botocore.exceptions.ClientError:
            logger.error(f"Failed to download {full_path} due to permission issues")
            raise
        except Exception:
            logger.error(f"Failed to download {full_path}")
            raise
        else:
            logger.info(f"Successfully downloaded {full_path}")
            buffer.seek(0)

        return buffer
