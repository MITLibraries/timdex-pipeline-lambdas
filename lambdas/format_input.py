import json
import logging
import uuid
from datetime import UTC, datetime

from lambdas import alma_prep, commands, errors, helpers
from lambdas.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def lambda_handler(event: dict, _context: dict) -> dict:
    """Format data into the necessary input for TIMDEX pipeline processing."""
    verbose = CONFIG.check_verbosity(event.get("verbose", False))
    CONFIG.configure_logger(logging.getLogger(), verbose=verbose)
    logger.debug(json.dumps(event))

    CONFIG.validate_input(event)

    run_date = helpers.format_run_date(event["run-date"])
    run_type = event["run-type"]
    source = event["source"]
    next_step = event["next-step"]
    run_id = event.get("run-id", str(uuid.uuid4()))
    run_timestamp = event.get("run-timestamp", datetime.now(UTC).isoformat())

    result = {
        "run-date": run_date,
        "run-type": run_type,
        "source": source,
        "verbose": verbose,
    }

    if next_step == "extract":
        if source in CONFIG.GIS_SOURCES:
            result["harvester-type"] = "geo"
        else:
            result["harvester-type"] = "oai"
        result["next-step"] = "transform"
        result["extract"] = commands.generate_extract_command(
            event,
            run_date,
            CONFIG.timdex_bucket,
            verbose,
        )
        return result

    if next_step == "transform":
        try:
            if source == "alma":
                alma_prep.prepare_alma_export_files(
                    run_date,
                    run_type,
                    CONFIG.timdex_bucket,
                )
            extract_output_files = helpers.list_s3_files_by_prefix(
                CONFIG.timdex_bucket,
                helpers.generate_step_output_prefix(
                    source,
                    run_date,
                    run_type,
                    "extract",
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
            extract_output_files,
            event,
            CONFIG.timdex_bucket,
            run_id,
            run_timestamp,
        )
        return result

    if next_step == "load":
        if not helpers.dataset_records_exist_for_run(
            CONFIG.timdex_bucket,
            run_date,
            run_id,
        ):
            result["failure"] = (
                "No records were found in the TIMDEX dataset for run_date "
                f"'{run_date}', run_id '{run_id}'."
            )
            return result
        result["load"] = commands.generate_load_commands(
            source,
            run_date,
            run_type,
            run_id,
            CONFIG.timdex_bucket,
        )
        return result

    message = f"'next-step' not supported: '{next_step}'"
    raise ValueError(message)
