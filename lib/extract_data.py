import requests
import logging
from typing import Dict
from lambda_utils.s3 import S3ClientInterface, S3Client
import lambda_utils.sns as sns
import lambda_utils.time as time_utils
import lambda_utils.logger as logging_utils
from params import IMDB_DATASETS

logger: logging.Logger = logging_utils.get_logger(module=__name__)


def extract_data(event, context):
    logger.debug(event)
    run_year_month_day: Dict[str, int] = time_utils.get_runtime_year_month_day()
    s3_client: S3ClientInterface = S3Client(bucket_name="imdb-dataset-raw-zone")
    for dataset in IMDB_DATASETS:
        write_imdb_dataset_to_s3(
            dataset_name=dataset,
            s3_client=s3_client,
            run_year_month_day=run_year_month_day,
        )

    return {"statusCode": 200, "body": "Success!"}


def send_get_request(url: str) -> requests.models.Response:
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request for {url=} failed due to permission issues")
        raise
    except Exception:
        logger.error(f"HTTP request for {url=} failed")
        raise
    else:
        logger.info(f"Successfully sent HTTP request for {url=}")
        return response


def write_imdb_dataset_to_s3(
    *,
    dataset_name: str,
    s3_client: S3ClientInterface,
    run_year_month_day: Dict[str, int],
) -> None:
    response: requests.models.Response = send_get_request(
        f"https://datasets.imdbws.com/{dataset_name}.tsv.gz"
    )

    year: int = run_year_month_day["year"]
    month: int = run_year_month_day["month"]
    day: int = run_year_month_day["day"]

    s3_client.upload_file_to_s3(
        byte_file=response.content,
        s3_key=f"imdb_raw_datasets/{dataset_name}/year={year}/month={month}/day={day}/file.tsv.gz",
    )
