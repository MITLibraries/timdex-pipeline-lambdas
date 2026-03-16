import logging
import uuid
from typing import TYPE_CHECKING

from lambdas import helpers
from lambdas.config import Config

if TYPE_CHECKING:
    from lambdas.format_input import InputPayload

logger = logging.getLogger(__name__)

CONFIG = Config()

GPU_RECORD_COUNT_THRESHOLD = 500


def generate_extract_command(input_payload: "InputPayload") -> dict:
    step = "extract"

    # prepare output filename(s)
    s3_output_uri = helpers.generate_s3_output_uri(input_payload, step)

    # initialize command
    cmd: list[str] = []

    if input_payload.verbose:
        cmd.append("--verbose")

    if input_payload.source in CONFIG.SOURCE_HARVESTER["geo"]:
        cmd = _generate_geoharvester_extract_command(
            cmd,
            input_payload,
            s3_output_uri,
        )
    elif input_payload.source in CONFIG.SOURCE_HARVESTER["browsertrix"]:
        cmd = _generate_browsertrix_harvester_extract_command(
            cmd,
            input_payload,
            s3_output_uri,
        )
    elif input_payload.source in CONFIG.SOURCE_HARVESTER["oai"]:
        cmd = _generate_oai_harvester_extract_command(
            cmd,
            input_payload,
            s3_output_uri,
        )
    else:
        raise RuntimeError(
            f"No configured harvester for source '{input_payload.source}'."
        )

    return {"extract-command": cmd}


def _generate_geoharvester_extract_command(
    cmd: list,
    input_payload: "InputPayload",
    s3_output_uri: str,
) -> list:
    cmd.append("harvest")

    if input_payload.run_type == "daily":
        cmd.extend(
            ["--harvest-type=incremental", f"--from-date={input_payload.from_date}"]
        )
    elif input_payload.run_type == "full":
        cmd.append("--harvest-type=full")

    cmd.extend(
        [f"--output-file={s3_output_uri}", input_payload.source.removeprefix("gis")]
    )

    return cmd


def _generate_browsertrix_harvester_extract_command(
    cmd: list,
    input_payload: "InputPayload",
    s3_output_uri: str,
) -> list:
    cmd.extend(
        [
            "harvest",
            f"--config-yaml-file={input_payload.raw['btrix-config-yaml-file']}",
            f"--records-output-file={s3_output_uri}",
        ]
    )

    if sitemaps := input_payload.raw.get("btrix-sitemaps"):
        cmd.extend(f"--sitemap={s}" for s in sitemaps)

    if input_payload.run_type == "daily":
        cmd.append(f"--sitemap-from-date={input_payload.from_date}")

    if sitemap_urls_out := input_payload.raw.get("btrix-sitemap-urls-output-file"):
        cmd.append(f"--sitemap-urls-output-file={sitemap_urls_out}")

    if sitemap_urls_previous := input_payload.raw.get("btrix-previous-sitemap-urls-file"):
        cmd.append(f"--previous-sitemap-urls-file={sitemap_urls_previous}")

    if btrix_args_json := input_payload.raw.get("btrix-args-json"):
        cmd.append(f"""--btrix-args-json={btrix_args_json}""")

    return cmd


def _generate_oai_harvester_extract_command(
    cmd: list,
    input_payload: "InputPayload",
    s3_output_uri: str,
) -> list:
    cmd.extend(
        [
            f"--host={input_payload.raw['oai-pmh-host']}",
            f"--output-file={s3_output_uri}",
            "harvest",
        ]
    )

    if input_payload.source in {"aspace", "dspace"}:
        cmd.append("--method=get")

    cmd.append(f"--metadata-format={input_payload.raw['oai-metadata-format']}")

    if input_payload.run_type == "daily":
        cmd.append(f"--from-date={input_payload.from_date}")
    elif input_payload.run_type == "full":
        cmd.append("--exclude-deleted")

    if set_spec := input_payload.raw.get("oai-set-spec"):
        cmd.append(f"--set-spec={set_spec}")

    return cmd


def generate_transform_commands(
    input_payload: "InputPayload",
    extract_output_files: list[str],
) -> dict[str, list[dict]]:
    """Generate task run command for TIMDEX transform."""
    files_to_transform: list[dict] = []
    for extract_output_file in extract_output_files:
        transform_command = [
            f"--input-file=s3://{CONFIG.timdex_bucket}/{extract_output_file}",
            f"--output-location={CONFIG.s3_timdex_dataset_location}",
            f"--source={input_payload.source}",
            f"--run-id={input_payload.run_id}",
            f"--run-timestamp={input_payload.run_timestamp}",
        ]
        if input_payload.source in CONFIG.source_exclusion_lists:
            transform_command.append(
                f"--exclusion-list-path={CONFIG.source_exclusion_lists[input_payload.source]}"
            )
        files_to_transform.append({"transform-command": transform_command})
    return {"files-to-transform": files_to_transform}


def generate_load_commands(input_payload: "InputPayload") -> dict:
    """Generate task run command for TIMDEX load."""
    update_command = [
        "bulk-update",
        "--run-date",
        input_payload.run_date,
        "--run-id",
        input_payload.run_id,
    ]

    if input_payload.run_type == "daily":
        update_command.extend(
            [
                "--source",
                input_payload.source,
                CONFIG.s3_timdex_dataset_location,
            ]
        )
        return {"bulk-update-command": update_command}

    if input_payload.run_type == "full":
        new_index_name = helpers.generate_index_name(input_payload.source)
        update_command.extend(
            ["--index", new_index_name, CONFIG.s3_timdex_dataset_location]
        )
        promote_index_command = ["promote", "--index", new_index_name]
        for alias, sources in CONFIG.INDEX_ALIASES.items():
            if input_payload.source in sources:
                promote_index_command.append("--alias")
                promote_index_command.append(alias)
        return {
            "create-index-command": ["create", "--index", new_index_name],
            "bulk-update-command": update_command,
            "promote-index-command": promote_index_command,
        }

    return {"failure": f"Unexpected run-type: '{input_payload.run_type}'"}


def generate_embeddings_create_command(
    input_payload: "InputPayload",
    record_count: int,
) -> dict:
    """Generate AWS Batch job parameters for creating embeddings.

    Determines compute environment based on record count:
    - cpu (ECS Fargate) for < 500 records
    - gpu-spot (EC2 Spot) for >= 500 records
    """
    job_compute_env = "gpu-spot" if record_count >= GPU_RECORD_COUNT_THRESHOLD else "cpu"

    return {
        "create": {
            "job_name": f"create-embeddings-{job_compute_env}-{uuid.uuid4()}",
            "job_compute_env": job_compute_env,
            "command": [
                "--verbose",
                "create-embeddings",
                "--strategy=full_record",
                f"--dataset-location={CONFIG.s3_timdex_dataset_location}",
                f"--run-id={input_payload.run_id}",
            ],
        }
    }


def generate_embeddings_load_command(input_payload: "InputPayload") -> dict:
    """Generate TIM command to update documents with embeddings."""
    return {
        "load": {
            "bulk-update-embeddings-command": [
                "--verbose",
                "bulk-update-embeddings",
                f"--source={input_payload.source}",
                f"--run-id={input_payload.run_id}",
                CONFIG.s3_timdex_dataset_location,
            ],
        }
    }
