import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import boto3

from lambdas import config, errors

logger = logging.getLogger(__name__)


def format_run_date(input_date: str) -> str:
    """Format an input date string into a TIMDEX date string.

    Validate that a supplied date string matches one of our allowed input date formats
    and return a date string matching the TIMDEX date format used in file name and
    index names, YYYY-MM-DD.
    """
    input_date_object = None
    for date_format in config.VALID_DATE_FORMATS:
        try:
            input_date_object = datetime.strptime(input_date, date_format)
        except ValueError:
            pass
    if input_date_object:
        return input_date_object.strftime("%Y-%m-%d")
    raise ValueError(
        "Input 'run-date' value must be one of the following date string formats: "
        f"{config.VALID_DATE_FORMATS}. Value provided was '{input_date}'"
    )


def generate_harvest_from_date(run_date_string: str) -> str:
    """Return a the provided date string minus one day, used for daily harvests."""
    run_date = datetime.strptime(run_date_string, "%Y-%m-%d")
    harvest_from_date = run_date - timedelta(days=1)
    return harvest_from_date.strftime("%Y-%m-%d")


def generate_index_name(source: str) -> str:
    """Generate a TIMDEX index name following our index naming convention."""
    return f"{source}-{datetime.now().strftime('%Y-%m-%dt%H-%M-%S')}"


def generate_step_output_filename(
    load_type: str,
    prefix: str,
    step: str,
    sequence: Optional[str] = None,
) -> str:
    """Generate a full TIMDEX file name as used for pipeline files in S3.

    Given a filename prefix, load type (index or delete), step (extract, transform, or
    load), and optional file sequence number, generate a full file name.
    """
    sequence_suffix = f"_{sequence}" if sequence else ""
    if step == "extract":
        file_type = "xml"
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
) -> Tuple[str, Optional[str]]:
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
    s3_files = []
    try:
        for s3_object in (
            s3_object for page in pages for s3_object in page["Contents"]
        ):
            s3_files.append(s3_object["Key"])
    except KeyError as error:
        logger.error(
            "No files retrieved from bucket '%s' with prefix '%s'", bucket, prefix
        )
        raise errors.NoFilesError from error
    return s3_files
