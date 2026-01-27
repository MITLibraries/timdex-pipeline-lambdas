import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal

from lambdas import alma_prep, commands, errors, helpers
from lambdas.config import Config, configure_logger

logger = logging.getLogger(__name__)

CONFIG = Config()

type NextStep = Literal["extract", "transform", "load", "exit-ok", "exit-error", "end"]


@dataclass
class InputPayload:
    run_date: str
    run_type: str
    source: str
    next_step: NextStep
    run_id: str
    run_timestamp: str
    raw: dict
    verbose: bool = True

    @staticmethod
    def validate_input(input_data: dict) -> None:
        """Validate input to the lambda function.

        Ensures that all required input fields are present and contain valid data.
        """
        # All required fields are present
        if missing_fields := [
            field for field in CONFIG.REQUIRED_FIELDS if field not in input_data
        ]:
            message = (
                f"Input must include all required fields. "
                f"Missing fields: {missing_fields}"
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
        if input_data["next-step"] == "extract":
            missing_harvest_fields = None
            if input_data["source"] in CONFIG.GIS_SOURCES:
                pass  # Currently no specific GeoHarvester requirements
            elif input_data["source"] == "mitlibwebsite":
                missing_harvest_fields = set(
                    CONFIG.REQUIRED_BTRIX_HARVEST_FIELDS
                ).difference(set(input_data.keys()))
                # require previous sitemaps URLs argument for daily runs
                if (
                    input_data["run-type"] == "daily"
                    and "btrix-previous-sitemap-urls-file" not in input_data
                ):
                    message = (
                        "Field 'btrix-previous-sitemap-urls-file' "
                        "required when 'run-type=daily'"
                    )
                    raise ValueError(message)
            else:
                missing_harvest_fields = set(
                    CONFIG.REQUIRED_OAI_HARVEST_FIELDS
                ).difference(set(input_data.keys()))

            if missing_harvest_fields:
                message = (
                    "Input must include all required harvest fields when starting "
                    f"with harvest step. Missing fields: {list(missing_harvest_fields)}"
                )
                raise ValueError(message)

    @classmethod
    def from_event(cls, event: dict) -> "InputPayload":
        # extract verbosity and debug log the payload
        verbose = CONFIG.get_verbose_flag(event.get("verbose", False))
        configure_logger(logging.getLogger(), verbose=verbose)
        logger.debug(json.dumps(event))

        # validate event payload
        cls.validate_input(event)

        return cls(
            run_date=helpers.format_run_date(event["run-date"]),
            run_type=event["run-type"],
            source=event["source"],
            next_step=event["next-step"],
            run_id=event.get("run-id", str(uuid.uuid4())),
            run_timestamp=event.get("run-timestamp", datetime.now(UTC).isoformat()),
            raw=event,
            verbose=verbose,
        )


@dataclass
class ResultPayload:
    next_step: NextStep
    run_date: str
    run_type: str
    source: str
    verbose: bool = True
    harvester_type: str | None = None
    extract: dict | None = None
    transform: dict | None = None
    load: dict | None = None
    message: str | None = None

    @classmethod
    def from_input_payload(cls, input_payload: "InputPayload") -> "ResultPayload":
        return cls(
            next_step=input_payload.next_step,
            run_date=input_payload.run_date,
            run_type=input_payload.run_type,
            source=input_payload.source,
            verbose=input_payload.verbose,
        )

    def to_dict(self) -> dict:
        return {k.replace("_", "-"): v for k, v in asdict(self).items() if v is not None}


def lambda_handler(event: dict, _context: dict) -> dict:
    """Format data into the necessary input for TIMDEX pipeline processing."""
    # validate and parse input payload
    input_payload = InputPayload.from_event(event)

    # prepare result
    result = ResultPayload.from_input_payload(input_payload)

    if input_payload.next_step == "extract":
        result = handle_extract(input_payload, result)
    elif input_payload.next_step == "transform":
        result = handle_transform(input_payload, result)
    elif input_payload.next_step == "load":
        result = handle_load(input_payload, result)
    else:
        raise ValueError(f"'next-step' not supported: '{input_payload.next_step}'")

    return result.to_dict()


def handle_extract(input_payload: InputPayload, result: ResultPayload) -> ResultPayload:
    result.next_step = "transform"
    if input_payload.source in CONFIG.GIS_SOURCES:
        result.harvester_type = "geo"
    elif input_payload.source == "mitlibwebsite":
        result.harvester_type = "browsertrix"
    else:
        result.harvester_type = "oai"
    result.extract = commands.generate_extract_command(input_payload)
    return result


def handle_transform(input_payload: InputPayload, result: ResultPayload) -> ResultPayload:
    result.next_step = "load"
    try:
        if input_payload.source == "alma":
            alma_prep.prepare_alma_export_files(input_payload)
        extract_output_files = helpers.list_s3_files_by_prefix(
            CONFIG.timdex_bucket,
            helpers.generate_step_output_prefix(
                input_payload,
                "extract",
            ),
        )
    except errors.NoFilesError:
        if input_payload.source == "alma" or input_payload.run_type == "full":
            result.next_step = "exit-error"
            message = (
                "There were no transformed files present in the TIMDEX S3 bucket "
                "for the provided date and source, something likely went wrong."
            )
            result.message = message
            logger.error(message)  # noqa: TRY400
        elif input_payload.run_type == "daily":
            result.next_step = "exit-ok"
            message = "There were no daily new/updated/deleted records to harvest."
            logger.info(message)
            result.message = message
        return result
    logger.info(
        "%s extracted files found in TIMDEX S3 bucket for date '%s' and source '%s'",
        len(extract_output_files),
        input_payload.run_date,
        input_payload.source,
    )
    result.transform = commands.generate_transform_commands(
        input_payload,
        extract_output_files,
    )
    return result


def handle_load(input_payload: InputPayload, result: ResultPayload) -> ResultPayload:
    result.next_step = "end"
    if not helpers.dataset_records_exist_for_run(input_payload.run_id):
        result.next_step = "exit-ok"
        message = (
            f"No transformed records to index or delete were found "
            f"for run_id '{input_payload.run_id}'."
        )
        logger.warning(message)
        result.message = message
        return result
    result.load = commands.generate_load_commands(input_payload)
    return result
