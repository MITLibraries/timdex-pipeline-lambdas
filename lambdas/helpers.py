import contextlib
import logging
from datetime import UTC, datetime, timedelta

import boto3
from timdex_dataset_api.dataset import TIMDEXDataset  # type: ignore[import-untyped]

from lambdas import errors
from lambdas.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def validate_input(input_data: dict) -> None:
    """Validate input to the lambda function.

    Ensures that all required input fields are present and contain valid data.
    """
    # All required fields are present
    if missing_fields := [
        field for field in CONFIG.REQUIRED_FIELDS if field not in input_data
    ]:
        message = (
            f"Input must include all required fields. Missing fields: {missing_fields}"
        )
        raise ValueError(message)

    # Valid next step
    next_step = input_data["next-step"]
    if next_step not in CONFIG.VALID_STEPS:
        message = (
            f"Input 'next-step' value must be one of: {CONFIG.VALID_STEPS}. Value "
            f"provided was '{next_step}'"
        )
        raise ValueError(message)

    # Valid run type
    run_type = input_data["run-type"]
    if run_type not in CONFIG.VALID_RUN_TYPES:
        message = (
            f"Input 'run-type' value must be one of: {CONFIG.VALID_RUN_TYPES}. Value "
            f"provided was '{run_type}'"
        )
        raise ValueError(message)

    # If next step is extract step, required harvest fields are present
    # ruff: noqa: SIM102
    if input_data["next-step"] == "extract":
        if input_data["source"] not in CONFIG.GIS_SOURCES:
            if missing_harvest_fields := [
                field
                for field in CONFIG.REQUIRED_OAI_HARVEST_FIELDS
                if field not in input_data
            ]:
                message = (
                    "Input must include all required harvest fields when starting "
                    f"with harvest step. Missing fields: {missing_harvest_fields}"
                )
                raise ValueError(message)


def format_run_date(input_date: str) -> str:
    """Format an input date string into a TIMDEX date string.

    Validate that a supplied date string matches one of our allowed input date formats
    and return a date string matching the TIMDEX date format used in file name and
    index names, YYYY-MM-DD.
    """
    input_date_object = None
    for date_format in CONFIG.VALID_DATE_FORMATS:
        with contextlib.suppress(ValueError):
            input_date_object = datetime.strptime(input_date, date_format).astimezone(UTC)
    if input_date_object:
        return input_date_object.strftime("%Y-%m-%d")
    message = (
        "Input 'run-date' value must be one of the following date string formats: "
        f"{CONFIG.VALID_DATE_FORMATS}. Value provided was '{input_date}'"
    )
    raise ValueError(message)


def generate_harvest_from_date(run_date_string: str) -> str:
    """Return a the provided date string minus one day, used for daily harvests."""
    run_date = datetime.strptime(run_date_string, "%Y-%m-%d").astimezone(UTC)
    harvest_from_date = run_date - timedelta(days=1)
    return harvest_from_date.strftime("%Y-%m-%d")


def generate_index_name(source: str) -> str:
    """Generate a TIMDEX index name following our index naming convention."""
    return f"{source}-{datetime.now(tz=UTC).strftime('%Y-%m-%dt%H-%M-%S')}"


def generate_step_output_filename(
    source: str,
    load_type: str,
    prefix: str,
    step: str,
    sequence: str | None = None,
) -> str:
    """Generate a full TIMDEX file name as used for pipeline files in S3.

    Given a filename prefix, load type (index or delete), step (extract, transform, or
    load), and optional file sequence number, generate a full file name.
    """
    sequence_suffix = f"_{sequence}" if sequence else ""
    if step == "extract":
        file_type = "jsonl" if source in CONFIG.GIS_SOURCES else "xml"
    elif load_type == "delete":
        file_type = "txt"
    else:
        file_type = "json"
    return f"{prefix}-to-{load_type}{sequence_suffix}.{file_type}"


def generate_step_output_prefix(
    source: str, formatted_run_date: str, run_type: str, step: str
) -> str:
    """Generate a TIMDEX file prefix as used for pipeline files in S3.

    Given a source, date string, run type (daily or full), and step (extract,
    transform, or harvest), generate a file prefix.
    """
    return f"{source}/{source}-{formatted_run_date}-{run_type}-{step}ed-records"


def get_load_type_and_sequence_from_timdex_filename(
    file_name: str,
) -> tuple[str, str | None]:
    """Given a TIMDEX file name, return the load type and optional sequence number.

    Returns:
        load_type: one of: index, delete
        sequence: zero-padded two digit file sequence number if present, otherwise None
    """
    name_parts = file_name.split(".")[0].split("_")
    load_type = name_parts[0].split("-")[-1]
    sequence = name_parts[1] if len(name_parts) > 1 else None
    return (load_type, sequence or None)


def list_s3_files_by_prefix(bucket: str, prefix: str) -> list[str]:
    """List all filenames with the provided prefix in the provided bucket."""
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    try:
        s3_files = [s3_object["Key"] for page in pages for s3_object in page["Contents"]]
    except KeyError as error:
        # ruff: noqa: TRY400
        logger.error(
            "No files retrieved from bucket '%s' with prefix '%s'", bucket, prefix
        )
        raise errors.NoFilesError from error
    return s3_files


def dataset_records_exist_for_run(run_date: str, run_id: str) -> bool:
    """Query TIMDEX dataset to confirm records to load and/or delete.

    A "run" is defined by a run-date + run-id, both provided as inputs to this lambda
    invocation provided by the StepFunction.  We are interested only in records where
    action is "index" or "delete".  If zero records exist, or have action "skip" or
    "error", we do not need to perform any load commands.
    """
    td = TIMDEXDataset(location=CONFIG.s3_timdex_dataset_location)
    td.load(run_date=run_date, run_id=run_id, action=["index", "delete"])
    return td.row_count > 0
