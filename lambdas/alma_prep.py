import logging
import tarfile
from collections.abc import Generator
from typing import IO, TYPE_CHECKING

import boto3
import smart_open  # type: ignore[import]

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client  # pragma: no cover

    from lambdas.format_input import InputPayload

from lambdas import helpers
from lambdas.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def extract_file_from_source_bucket_to_target_bucket(
    s3_client: "S3Client",
    source_bucket: str,
    source_file_key: str,
    target_bucket: str,
    target_file_key: str,
) -> None:
    """Extract a single tarred file from one s3 bucket to another s3 bucket."""
    transport_params = {"client": s3_client}
    with smart_open.open(
        f"s3://{source_bucket}/{source_file_key}",
        "rb",
        transport_params=transport_params,
    ) as tar_file:
        logger.debug("Extracting file '%s'", source_file_key)
        file_contents = next(extract_tarfile(tar_file))
        with smart_open.open(
            f"s3://{target_bucket}/{target_file_key}",
            "wb",
            transport_params=transport_params,
        ) as out_file:
            while True:
                chunk = file_contents.read(8 * 1024 * 1024)
                if not chunk:
                    break
                out_file.write(chunk)
        logger.debug(
            "File '%s' extracted from bucket '%s' and uploaded to bucket '%s' with new "
            "file name %s",
            source_file_key,
            source_bucket,
            target_bucket,
            target_file_key,
        )


def extract_tarfile(tar_file: IO[bytes]) -> Generator[IO[bytes], None, None]:
    """Extract the contents of a tarfile and yield each member."""
    with tarfile.open(fileobj=tar_file) as tar:
        for member in tar.getmembers():
            contents = tar.extractfile(member)
            if contents:
                yield contents


def get_load_type_and_sequence_from_alma_export_filename(
    export_file_name: str,
) -> tuple[str, str | None]:
    """Get the load type and sequence from an Alma export filename.

    Args:
        export_file_name: An Alma export filename following the expected naming
            convention: TIMDEX_ALMA_EXPORT_<run-type>_<timestamp>[<job>]_
            <load-type>_<optional-sequence-number>.xml

    Returns:
        tuple: A string representing the load type - "delete" for files of records to be
            deleted or "index" for files of either new or updated records to be
            indexed, and either the file sequence number if the export contained
            multiple files or None if no sequence number is present.
    """
    name_parts = export_file_name.split(".")[0].split("_")
    last_part = name_parts[-1]
    if last_part.isdigit():
        sequence = last_part.zfill(2)
        load_type = name_parts[-2]
    else:
        sequence = None
        load_type = last_part
    if load_type in ["new", "update"]:
        load_type = "index"
    return (load_type, sequence)


def prepare_alma_export_files(input_payload: "InputPayload") -> None:
    """Extract and unzip alma export files to the TIMDEX S3 bucket.

    Alma files are exported to an SFTP S3 bucket as gzipped tarfiles. Prior to the
    pipeline transform and load steps, these need to be unzipped, the XML files
    extracted, renamed following the TIMDEX pipeline file naming convention, and
    uploaded to the TIMDEX S3 bucket. This function identifies the Alma files from a
    given export using the export job date and expected export file naming convention,
    then performs the extract, unzip, rename and upload steps.
    """
    export_job_date = input_payload.run_date.replace("-", "")
    alma_bucket = CONFIG.alma_export_bucket
    alma_export_files = helpers.list_s3_files_by_prefix(
        alma_bucket,
        f"exlibris/timdex/TIMDEX_ALMA_EXPORT_{input_payload.run_type.upper()}_{export_job_date}",
    )
    logger.info(
        "%s Alma export files found in S3 for date %s",
        len(alma_export_files),
        input_payload.run_date,
    )
    s3_client = boto3.client("s3")
    for export_file in alma_export_files:
        load_type, sequence = get_load_type_and_sequence_from_alma_export_filename(
            export_file
        )
        extract_output_file = helpers.generate_step_output_filename(
            "alma",
            load_type,
            helpers.generate_step_output_prefix(input_payload, "extract"),
            "extract",
            sequence,
        )
        extract_file_from_source_bucket_to_target_bucket(
            s3_client, alma_bucket, export_file, CONFIG.timdex_bucket, extract_output_file
        )
