import datetime
import json
import logging
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

env = os.getenv("WORKSPACE")
if sentry_dsn := os.getenv("SENTRY_DSN"):
    sentry = sentry_sdk.init(
        dsn=sentry_dsn,
        environment=env,
        integrations=[
            AwsLambdaIntegration(),
        ],
        traces_sample_rate=1.0,
    )
    logger.info("Sentry DSN found, exceptions will be sent to Sentry with env=%s", env)
else:
    logger.info("No Sentry DSN found, exceptions will not be sent to Sentry")

REQUIRED_FIELDS = (
    "harvest-type",
    "opensearch-url",
    "output-bucket",
    "starting-step",
    "source",
    "time",
)

REQUIRED_HARVEST_FIELDS = ("oai-pmh-host", "oai-metadata-format")
VALID_DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ")
VALID_HARVEST_TYPES = ("full", "daily")
VALID_STARTING_STEPS = ("harvest", "transform", "load")


def lambda_handler(event: dict, context: dict) -> dict:  # noqa
    logger.debug(json.dumps(event))

    if not os.getenv("WORKSPACE"):
        raise RuntimeError("Required env variable WORKSPACE is not set")

    validate_input(event)

    harvest_end_date = generate_harvest_end_date_string(event["time"])
    harvest_type = event["harvest-type"]
    source = event["source"]
    starting_step = event["starting-step"]
    harvest_output_file = (
        f"{source}-{harvest_type}-harvested-records-{harvest_end_date}.xml"
    )
    transform_output_file = (
        f"{source}-{harvest_type}-transformed-records-{harvest_end_date}.json"
    )

    output = {
        "starting-step": starting_step,
        "load": generate_load_input(event, harvest_type, transform_output_file),
    }

    if starting_step in ["transform", "harvest"]:
        output["transform"] = generate_transform_input(
            event, harvest_output_file, transform_output_file
        )

    if starting_step == "harvest":
        output["harvest"] = generate_harvest_input(
            event, harvest_end_date, harvest_type, harvest_output_file
        )

    return output


def validate_input(event: dict) -> None:

    # All required fields are present
    if missing_fields := [field for field in REQUIRED_FIELDS if field not in event]:
        raise ValueError(
            f"Input must include all required fields. Missing fields: {missing_fields}"
        )

    # Valid starting step
    starting_step = event["starting-step"]
    if starting_step not in VALID_STARTING_STEPS:
        raise ValueError(
            f"Input 'starting-step' value must be one of: {VALID_STARTING_STEPS}. "
            f"Value provided was '{starting_step}'"
        )

    # Valid harvest type
    harvest_type = event["harvest-type"]
    if harvest_type not in VALID_HARVEST_TYPES:
        raise ValueError(
            f"Input 'harvest-type' value must be one of: {VALID_HARVEST_TYPES}. Value "
            f"provided was '{harvest_type}'"
        )

    # If starting with harvest step, required harvest fields are present
    if starting_step == "harvest":
        if missing_harvest_fields := [
            field for field in REQUIRED_HARVEST_FIELDS if field not in event
        ]:
            raise ValueError(
                "Input must include all required harvest fields when starting with "
                f"harvest step. Missing fields: {missing_harvest_fields}"
            )


def generate_harvest_end_date_string(input_date: str) -> str:
    input_date_object = None
    for date_format in VALID_DATE_FORMATS:
        try:
            input_date_object = datetime.datetime.strptime(input_date, date_format)
        except ValueError:
            pass
    if input_date_object:
        harvest_end_date = input_date_object - datetime.timedelta(days=1)
        return harvest_end_date.strftime("%Y-%m-%d")
    raise ValueError(
        "Input 'time' value must be one of the following date string formats: "
        f"{VALID_DATE_FORMATS}. Value provided was '{input_date}'"
    )


def generate_load_input(
    event: dict, harvest_type: str, transform_output_file: str
) -> dict:
    if prefix := event.get("index-prefix"):
        index_name = f"{prefix}-{event['source']}"
    else:
        index_name = event["source"]

    load_commands = [
        f"--url={event['opensearch-url']}",
        "ingest",
        f"--source={index_name}",
    ]

    if harvest_type == "full":
        load_commands.append("--new")
        if prefix != "rdi":
            load_commands.append("--auto")

    load_commands.append(f"s3://{event['output-bucket']}/{transform_output_file}")

    return {"commands": load_commands}


def generate_transform_input(
    event: dict, harvest_output_file: str, transform_output_file: str
) -> dict:
    transform_commands = [
        f"--input-file=s3://{event['output-bucket']}/{harvest_output_file}",
        f"--output-file=s3://{event['output-bucket']}/{transform_output_file}",
        f"--source={event['source']}",
    ]

    if event.get("verbose") == "true".lower():
        transform_commands.append("--verbose")

    return {
        "commands": transform_commands,
        "result-file": {
            "bucket": event["output-bucket"],
            "key": transform_output_file,
        },
    }


def generate_harvest_input(
    event: dict, harvest_end_date: str, harvest_type: str, harvest_output_file: str
) -> dict:
    harvest_commands = [
        f"--host={event['oai-pmh-host']}",
        f"--output-file=s3://{event['output-bucket']}/{harvest_output_file}",
    ]

    if event.get("verbose") == "true".lower():
        harvest_commands.append("--verbose")

    harvest_commands.append("harvest")

    if event["source"] == "aspace":
        harvest_commands.append("--method=get")

    harvest_commands.append(f"--metadata-format={event['oai-metadata-format']}")

    if event.get("oai-set-spec"):
        harvest_commands.append(f"--set-spec={event['oai-set-spec']}")

    if harvest_type == "daily":
        harvest_commands.append(
            f"--from-date={harvest_end_date}",
        )

    harvest_commands.extend(
        [
            f"--until-date={harvest_end_date}",
            "--exclude-deleted",
        ]
    )

    return {
        "commands": harvest_commands,
        "result-file": {
            "bucket": event["output-bucket"],
            "key": harvest_output_file,
        },
    }
