# ruff: noqa: PLR0911, PLR2004

import json
import logging
import os
import uuid

from lambdas import alma_prep, commands, config, errors, helpers

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, _context: dict) -> dict:
    """Format data into the necessary input for TIMDEX pipeline processing."""
    config.verify_env()
    verbose = config.check_verbosity(event.get("verbose", False))
    config.configure_logger(logging.getLogger(), verbose)
    logger.debug(json.dumps(event))
    config.validate_input(event)

    run_date = helpers.format_run_date(event["run-date"])
    run_type = event["run-type"]
    source = event["source"]
    next_step = event["next-step"]
    run_id = event.get("run-id", str(uuid.uuid4()))
    timdex_bucket = os.environ["TIMDEX_S3_EXTRACT_BUCKET_ID"]

    result = {
        "run-date": run_date,
        "run-type": run_type,
        "source": source,
        "verbose": verbose,
    }

    if next_step == "extract":
        if source in config.GIS_SOURCES:
            result["harvester-type"] = "geo"
        else:
            result["harvester-type"] = "oai"
        result["next-step"] = "transform"
        result["extract"] = commands.generate_extract_command(
            event, run_date, timdex_bucket, verbose
        )
        return result

    if next_step == "transform":
        try:
            if source == "alma":
                alma_prep.prepare_alma_export_files(run_date, run_type, timdex_bucket)
            extract_output_files = helpers.list_s3_files_by_prefix(
                timdex_bucket,
                helpers.generate_step_output_prefix(
                    source, run_date, run_type, "extract"
                ),
            )
        except errors.NoFilesError:
            if source == "alma" or run_type == "full":
                result["failure"] = (
                    "There were no transformed files present in the TIMDEX S3 bucket "
                    "for the provided date and source, something likely went wrong."
                )
            elif run_type == "daily":
                result["success"] = (
                    "There were no daily new/updated/deleted records to harvest."
                )
            return result
        logger.info(
            "%s extracted files found in TIMDEX S3 bucket for date '%s' and source '%s'",
            len(extract_output_files),
            run_date,
            source,
        )
        result["next-step"] = "load"
        result["transform"] = commands.generate_transform_commands(
            extract_output_files, event, run_date, timdex_bucket, run_id
        )
        return result

    if next_step == "load":
        # NOTE: FEATURE FLAG: branching logic will be removed after v2 work is complete
        etl_version = config.get_etl_version()

        if etl_version == 1:
            try:
                transform_output_files = helpers.list_s3_files_by_prefix(
                    timdex_bucket,
                    helpers.generate_step_output_prefix(
                        source, run_date, run_type, "transform"
                    ),
                )
                result["load"] = commands.generate_load_commands_v1(
                    transform_output_files, run_type, source, timdex_bucket
                )
                return result  # noqa: TRY300
            except errors.NoFilesError:
                result["failure"] = (
                    "There were no transformed files present in the TIMDEX S3 bucket for "
                    "the provided date and source, something likely went wrong."
                )
                return result

        elif etl_version == 2:
            if not helpers.dataset_records_exist_for_run(timdex_bucket, run_date, run_id):
                result["failure"] = (
                    "No records were found in the TIMDEX dataset for run_date "
                    f"'{run_date}', run_id '{run_id}'."
                )
                return result
            result["load"] = commands.generate_load_commands(
                source, run_date, run_type, run_id, timdex_bucket
            )
            return result

    message = f"'next-step' not supported: '{next_step}'"
    raise ValueError(message)
