import os
from typing import Dict, Union


def get_aws_credentials() -> Dict[str, Union[str, None]]:
    aws_credentials: Dict[str, Union[str, None]] = dict()
    aws_credentials["aws_access_key"] = os.getenv("aws_access_key")
    aws_credentials["aws_secret_access_key"] = os.getenv("aws_secret_access_key")
    return aws_credentials
