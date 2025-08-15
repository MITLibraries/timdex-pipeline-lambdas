import logging

from lambdas import helpers
from lambdas.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def generate_extract_command(
    # ruff: noqa: FBT001
    input_data: dict,
    run_date: str,
    timdex_bucket: str,
    verbose: bool,
) -> dict:
    """Generate task run command for TIMDEX extract."""
    run_type = input_data["run-type"]
    source = input_data["source"]
    step = "extract"
    extract_output_prefix = helpers.generate_step_output_prefix(
        source, run_date, run_type, step
    )
    extract_output_file = helpers.generate_step_output_filename(
        source, "index", extract_output_prefix, step
    )

    extract_command = []

    if verbose:
        extract_command.append("--verbose")

    if source in CONFIG.GIS_SOURCES:
        extract_command.append("harvest")
        if run_type == "daily":
            extract_command.append("--harvest-type=incremental")
            extract_command.append(
                f"--from-date={helpers.generate_harvest_from_date(run_date)}"
            )
        elif run_type == "full":
            extract_command.append("--harvest-type=full")

        extract_command.append(
            f"--output-file=s3://{timdex_bucket}/{extract_output_file}"
        )
        extract_command.append(source.removeprefix("gis"))

    else:
        extract_command.append(f"--host={input_data['oai-pmh-host']}")
        extract_command.append(
            f"--output-file=s3://{timdex_bucket}/{extract_output_file}"
        )
        extract_command.append("harvest")
        if source in ["aspace", "dspace"]:
            extract_command.append("--method=get")
        extract_command.append(f"--metadata-format={input_data['oai-metadata-format']}")
        if run_type == "daily":
            extract_command.append(
                f"--from-date={helpers.generate_harvest_from_date(run_date)}",
            )
        elif run_type == "full":
            extract_command.append("--exclude-deleted")

        if set_spec := input_data.get("oai-set-spec"):
            extract_command.append(f"--set-spec={set_spec}")

    return {
        "extract-command": extract_command,
    }


def generate_transform_commands(
    extract_output_files: list[str],
    input_data: dict,
    timdex_bucket: str,
    run_id: str,
    run_timestamp: str,
) -> dict[str, list[dict]]:
    """Generate task run command for TIMDEX transform."""
    files_to_transform: list[dict] = []
    source = input_data["source"]
    for extract_output_file in extract_output_files:
        transform_command = [
            f"--input-file=s3://{timdex_bucket}/{extract_output_file}",
            f"--output-location={CONFIG.s3_timdex_dataset_location}",
            f"--source={source}",
            f"--run-id={run_id}",
            f"--run-timestamp={run_timestamp}",
        ]
        files_to_transform.append({"transform-command": transform_command})
    return {"files-to-transform": files_to_transform}


def generate_load_commands(
    source: str,
    run_date: str,
    run_type: str,
    run_id: str,
) -> dict:
    """Generate task run command for TIMDEX load."""
    update_command = [
        "bulk-update",
        "--run-date",
        run_date,
        "--run-id",
        run_id,
    ]

    if run_type == "daily":
        update_command.extend(
            [
                "--source",
                source,
                CONFIG.s3_timdex_dataset_location,
            ]
        )
        return {"bulk-update-command": update_command}

    if run_type == "full":
        new_index_name = helpers.generate_index_name(source)
        update_command.extend(
            ["--index", new_index_name, CONFIG.s3_timdex_dataset_location]
        )
        promote_index_command = ["promote", "--index", new_index_name]
        for alias, sources in CONFIG.INDEX_ALIASES.items():
            if source in sources:
                promote_index_command.append("--alias")
                promote_index_command.append(alias)
        return {
            "create-index-command": ["create", "--index", new_index_name],
            "bulk-update-command": update_command,
            "promote-index-command": promote_index_command,
        }

    return {"failure": f"Unexpected run-type: '{run_type}'"}
