# ruff: noqa: S608

import json
import logging
import uuid
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal

from timdex_dataset_api.dataset import TIMDEXDataset

from lambdas import alma_prep, commands, errors, helpers
from lambdas.config import Config, configure_logger

logger = logging.getLogger(__name__)

CONFIG = Config()

type NextStep = Literal[
    "extract",
    "transform",
    "load",
    "enrichment",
    "embeddings-create",
    "embeddings-load",
    "fulltexts-harvest",
    "fulltexts-load",
    "exit-ok",
    "exit-error",
    "finalize",
    "end",
]


ENRICHMENT_STEPS = {
    "enrichment",
    "embeddings-create",
    "embeddings-load",
    "fulltexts-harvest",
    "fulltexts-load",
}


@dataclass
class InputPayload:
    run_date: str
    run_type: Literal["daily", "full"]
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

        # If next step is extract step, required harvester fields are present
        if input_data["next-step"] == "extract":
            missing_harvest_fields = None

            harvester_type = None
            for configured_harvester_type, sources in CONFIG.SOURCE_HARVESTER.items():
                if input_data["source"] in sources:
                    harvester_type = configured_harvester_type
                    break

            if harvester_type == "geo":
                pass  # Currently no specific GeoHarvester requirements
            elif harvester_type == "browsertrix":
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

    @property
    def from_date(self) -> str:
        return helpers.generate_harvest_from_date(self.run_date)


@dataclass
class ResultPayload:
    next_step: NextStep
    run_date: str
    run_type: str
    run_id: str
    source: str
    verbose: bool = True
    harvester_type: str | None = None
    extract: dict | None = None
    transform: dict | None = None
    load: dict | None = None
    enrichments: dict | None = None
    embeddings: dict | None = None
    fulltexts: dict | None = None
    metrics: dict | None = None
    message: str | list | None = None

    @classmethod
    def from_input_payload(cls, input_payload: "InputPayload") -> "ResultPayload":
        return cls(
            next_step=input_payload.next_step,
            run_date=input_payload.run_date,
            run_type=input_payload.run_type,
            run_id=input_payload.run_id,
            source=input_payload.source,
            verbose=input_payload.verbose,
            enrichments=deepcopy(input_payload.raw.get("enrichments")),
        )

    def to_dict(self) -> dict:
        return {k.replace("_", "-"): v for k, v in asdict(self).items() if v is not None}


def lambda_handler(event: dict, _context: dict) -> dict:
    """Format data into the necessary input for TIMDEX pipeline processing."""
    # validate and parse input payload
    input_payload = InputPayload.from_event(event)

    # prepare result
    result = ResultPayload.from_input_payload(input_payload)

    # primary record ETL
    if input_payload.next_step == "extract":
        result = handle_extract(input_payload, result)
    elif input_payload.next_step == "transform":
        result = handle_transform(input_payload, result)
    elif input_payload.next_step == "load":
        result = handle_load(input_payload, result)

    # enrichments
    elif input_payload.next_step in ENRICHMENT_STEPS:
        result = handle_enrichment_steps(input_payload, result)

    # report and finalize
    elif input_payload.next_step == "finalize":
        result = handle_finalize(input_payload, result)

    else:
        raise ValueError(f"'next-step' not supported: '{input_payload.next_step}'")

    return result.to_dict()


def handle_extract(input_payload: InputPayload, result: ResultPayload) -> ResultPayload:
    result.next_step = "transform"

    for harvester_type, sources in CONFIG.SOURCE_HARVESTER.items():
        if input_payload.source in sources:
            result.harvester_type = harvester_type
            break
    else:
        raise RuntimeError(
            f"No configured harvester for source '{input_payload.source}'."
        )

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
    result.next_step = "enrichment"
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


def handle_enrichment_steps(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Route an enrichment step to its handler."""
    handlers = {
        "enrichment": handle_parallel_enrichments,
        "embeddings-create": handle_embeddings_create,
        "embeddings-load": handle_embeddings_load,
        "fulltexts-harvest": handle_fulltexts_harvest,
        "fulltexts-load": handle_fulltexts_load,
    }
    return handlers[input_payload.next_step](input_payload, result)


def handle_parallel_enrichments(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Prepare the enrichments payload for parallel enrichment work.

    Enrichments are performed via a 'Parallel' state in the StepFunction.  As such, the
    'next-step' for each branch in this parallel applies only to that branch, and is
    fully managed by that branch.  Once the branch completes, the results are shared,
    and any 'next-step' values used within that branch are dropped / ignored.
    """
    result.next_step = "finalize"

    # default enrichment shape
    enrichments: dict = {
        "skip": False,
        "embeddings": {"skip": False},
        "fulltexts": {"skip": False},
    }

    # overlay caller-supplied enrichment options, ensuring all branches have a "skip" key
    if caller_options := input_payload.raw.get("enrichments"):
        enrichments.update(deepcopy(caller_options))
        for branch in ("embeddings", "fulltexts"):
            enrichments[branch].setdefault("skip", False)

    result.enrichments = enrichments
    return result


def handle_embeddings_create(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Analyze ETL run and prepare parameters for AWS Batch job to create embeddings.

    There are currently three compute environments we can create embeddings in:
        - ECS Fargate - "cpu"
        - EC2 - "gpu"
        - EC2 Spot Instances - "gpu-spot"

    This lambda handler is responsible for analyzing the size and shape of the ETL run,
    and determining which AWS Batch compute environment is most appropriate.

    We do not create embeddings for all sources.  Those we skip are configured in
    CONFIG.SKIP_EMBEDDINGS_SOURCES.

    Additionally, at this time, we do not have a scenario or code path that would
    utilize the "gpu" compute environment, only "gpu-spot".  This is mostly because we
    don't require an immediate turnaround for embeddings creation; when the job size
    calls for a GPU, we have the luxury of waiting for a spot instance.
    """
    result.next_step = "embeddings-load"

    if input_payload.source in CONFIG.SKIP_EMBEDDINGS_SOURCES:
        result.next_step = "exit-ok"
        result.message = (
            f"Not currently creating embeddings for source '{input_payload.source}'"
        )
        return result

    # retrieve records count for run
    td = TIMDEXDataset(location=CONFIG.s3_timdex_dataset_location)
    record_count = td.conn.query(f"""
        select count(*)
        from metadata.records
        where run_id = '{input_payload.run_id}'
        and action in ('index')
        """).fetchone()[0]

    # exit early if no records to create embeddings for
    if record_count == 0:
        result.next_step = "exit-ok"
        result.message = f"No embeddable records found for run '{input_payload.run_id}'."
        return result

    job_compute_env = (
        "gpu-spot" if record_count >= commands.GPU_RECORD_COUNT_THRESHOLD else "cpu"
    )
    logger.info(
        f"ETL run '{input_payload.run_id}' had {record_count} records indexed, "
        f"recommending '{job_compute_env}' compute env."
    )

    result.embeddings = commands.generate_embeddings_create_command(
        input_payload, record_count
    )
    return result


def handle_embeddings_load(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Prepare TIM command to update documents in Opensearch with embeddings.

    We do not create embeddings for all sources.  Those we skip are configured in
    CONFIG.SKIP_EMBEDDINGS_SOURCES.

    Note that this is the terminal state in an enrichment branch, which is why we have
    next-step=end.
    """
    # This 'end' next-step refers to only the parallel enrichments branch
    result.next_step = "end"

    if input_payload.source in CONFIG.SKIP_EMBEDDINGS_SOURCES:
        result.next_step = "exit-ok"
        result.message = (
            f"Not currently indexing embeddings for source '{input_payload.source}'"
        )
        return result

    # retrieve embeddings count for run
    td = TIMDEXDataset(location=CONFIG.s3_timdex_dataset_location)
    embeddings_count = td.conn.query(f"""
        select count(*)
        from metadata.current_run_embeddings
        where run_id = '{input_payload.run_id}'
    """).fetchone()[0]

    # exit early if no embeddings to load
    if embeddings_count == 0:
        result.next_step = "exit-ok"
        result.message = f"No embeddings found for run '{input_payload.run_id}'."
        return result

    logger.info(
        f"Preparing TIM command to update {embeddings_count} documents with embeddings."
    )

    result.embeddings = commands.generate_embeddings_load_command(input_payload)
    return result


def handle_fulltexts_harvest(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Analyze ETL run and prepare CLI commands for fulltext harvesters.

    At this time, only a single fulltext harvester exists for the 'dspace' source.
    """
    result.next_step = "fulltexts-load"

    if input_payload.source not in CONFIG.VALID_FULLTEXTS_SOURCES:
        result.next_step = "exit-ok"
        result.message = (
            f"Not currently harvesting fulltexts for source '{input_payload.source}'"
        )
        return result

    # retrieve records count for run
    td = TIMDEXDataset(location=CONFIG.s3_timdex_dataset_location)
    record_count = td.conn.query(f"""
        select count(*)
        from metadata.records
        where run_id = '{input_payload.run_id}'
        and action in ('index')
        """).fetchone()[0]

    # exit early if no records to harvest fulltext for
    if record_count == 0:
        result.next_step = "exit-ok"
        result.message = (
            f"No records found for run '{input_payload.run_id}', no fulltexts to harvest."
        )
        return result

    result.fulltexts = commands.generate_fulltexts_harvest_command(input_payload)

    return result


def handle_fulltexts_load(
    input_payload: InputPayload, result: ResultPayload
) -> ResultPayload:
    """Load harvested fulltexts into Opensearch.

    Note that this is the terminal state in an enrichment branch, which is why we have
    next-step=end.
    """
    result.next_step = "end"

    # retrieve fulltexts count for run
    td = TIMDEXDataset(location=CONFIG.s3_timdex_dataset_location)
    fulltexts_count = td.conn.query(f"""
            select count(*)
            from metadata.current_run_fulltexts
            where run_id = '{input_payload.run_id}'
        """).fetchone()[0]

    # exit early if no fulltexts to load
    if fulltexts_count == 0:
        result.next_step = "exit-ok"
        result.message = f"No fulltexts found for run '{input_payload.run_id}'."
        return result

    logger.info(
        f"Preparing TIM command to update {fulltexts_count} documents with fulltexts."
    )

    result.fulltexts = commands.generate_fulltexts_load_command(input_payload)
    return result


def handle_finalize(input_payload: InputPayload, result: ResultPayload) -> ResultPayload:
    result.next_step = "end"

    # retrieve and log ETL run metrics
    run_metrics = helpers.get_run_metrics(input_payload.run_id)
    logger.info(f"Run metrics: {json.dumps(run_metrics)}")

    # WIP: next pass will publish these metrics as AWS Metrics right here

    # attach metrics to output
    result.metrics = run_metrics

    return result
