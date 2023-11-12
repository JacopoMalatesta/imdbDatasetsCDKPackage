import sys
from typing import Dict, List
import time
import logging

import boto3


def get_logger(module: str) -> logging.Logger:
    logger = logging.getLogger(name=module)
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(fmt=formatter)

    logger.addHandler(hdlr=handler)

    return logger


logger: logging.Logger = get_logger(__name__)


def _start_data_quality_checks(
    *,
    glue_client: boto3.client,
    ruleset_names: List[str],
    database_name: str,
    table_name: str,
    iam_role: str,
) -> str:
    logger.info(
        f"Starting DQ evaluation on {database_name}.{table_name} with {iam_role=}"
    )

    try:
        run_id: Dict[str, str] = glue_client.start_data_quality_ruleset_evaluation_run(
            DataSource={
                "GlueTable": {
                    "DatabaseName": database_name,
                    "TableName": table_name,
                }
            },
            Role=iam_role,
            RulesetNames=ruleset_names,
            AdditionalRunOptions={"CloudWatchMetricsEnabled": True},
        )
    except Exception as e:
        logger.error("Failed to start data quality ruleset evaluation")
        raise e
    return run_id["RunId"]


def _get_data_quality_results(*, glue_client: boto3.client, run_id: str):
    logger.info("Waiting for DQ results for {run_id}=")

    while True:
        response: Dict = glue_client.get_data_quality_ruleset_evaluation_run(
            RunId=run_id
        )
        status: str = response["Status"]

        if status in ["STOPPING", "STOPPED", "FAILED", "TIMEOUT"]:
            logger.error(
                f"Ruleset evalution failed with {status=} and error={response['ErrorString']}"
            )
            sys.exit(1)
        elif status == "SUCCEEDED":
            break
        time.sleep(15)

    result_ids: List[str] = response["ResultIds"]
    logger.info(f"Result IDs returned by DQ evaluation run: {result_ids}")
    return result_ids


def _log_data_quality_results(
    *, glue_client: boto3.client, result_ids: List[str]
) -> None:
    for result_id in result_ids:
        logger.info(f"Getting DQ results for {result_id=}")
        data_quality_results: Dict = glue_client.get_data_quality_result(
            ResultId=result_id
        )
        logger.info(
            f"Overall score for DQ evaluation run: {data_quality_results['Score']}"
        )
        rule_results: List[Dict[str, str]] = data_quality_results["RuleResults"]
        for rule_result in rule_results:
            logger.info(f"Rule={rule_result['Description']}\n{rule_result['Result']}")


def run_data_quality_ruleset_evaluation(
    *, ruleset_name_filter: str, glue_database: str, glue_table: str, iam_role: str
) -> None:
    glue_client = boto3.client("glue")

    available_rulesets: List[str] = glue_client.list_data_quality_rulesets(
        Filter={"Name": ruleset_name_filter}
    )["Rulesets"]

    ruleset_names: List[str] = [ruleset["Name"] for ruleset in available_rulesets]

    logger.info(f"Ruleset names being evaluated: {ruleset_names}")

    run_id: str = _start_data_quality_checks(
        glue_client=glue_client,
        ruleset_names=ruleset_names,
        database_name=glue_database,
        table_name=glue_table,
        iam_role=iam_role,
    )

    result_ids: List[str] = _get_data_quality_results(
        glue_client=glue_client, run_id=run_id
    )

    _log_data_quality_results(glue_client=glue_client, result_ids=result_ids)


if __name__ == "__main__":

    logger.info(f'boto3 version: {boto3.__version__}')

    run_data_quality_ruleset_evaluation(ruleset_name_filter="titleBasicsDataQualityRuleset",
                                        glue_database="imdb_raw_datasets",
                                        glue_table="title_basics",
                                        iam_role="arn:aws:iam::925000840053:role/GlueRole")
