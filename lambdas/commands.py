import logging

from lambdas import config, helpers

logger = logging.getLogger(__name__)


def generate_extract_command(
    # ruff: noqa: FBT001
    input_data: dict,
    run_date: str,
    timdex_bucket: str,
    verbose: bool,
) -> dict:
    """Generate task run command for extract via OAI-PMH harvest."""
    run_type = input_data["run-type"]
    source = input_data["source"]
    step = "extract"
    extract_output_prefix = helpers.generate_step_output_prefix(
        source, run_date, run_type, step
    )
    extract_output_file = helpers.generate_step_output_filename(
        "index", extract_output_prefix, step
    )

    extract_command = [
        f"--host={input_data['oai-pmh-host']}",
        f"--output-file=s3://{timdex_bucket}/{extract_output_file}",
    ]

    if verbose:
        extract_command.append("--verbose")

    extract_command.append("harvest")
    extract_command.append(f"--metadata-format={input_data['oai-metadata-format']}")

    if source in ["aspace", "dspace"]:
        extract_command.append("--method=get")

    if set_spec := input_data.get("oai-set-spec"):
        extract_command.append(f"--set-spec={set_spec}")

    if run_type == "daily":
        extract_command.append(
            f"--from-date={helpers.generate_harvest_from_date(run_date)}",
        )
    elif run_type == "full":
        extract_command.append("--exclude-deleted")

    return {
        "extract-command": extract_command,
    }


def generate_transform_commands(
    extract_output_files: list[str],
    input_data: dict,
    run_date: str,
    timdex_bucket: str,
) -> dict:
    """Generate task run command for TIMDEX transform."""
    files_to_transform: list[dict] = []

    source = input_data["source"]
    transform_output_prefix = helpers.generate_step_output_prefix(
        source, run_date, input_data["run-type"], "transform"
    )

    for extract_output_file in extract_output_files:
        load_type, sequence = helpers.get_load_type_and_sequence_from_timdex_filename(
            extract_output_file
        )
        transform_output_file = helpers.generate_step_output_filename(
            load_type, transform_output_prefix, "transform", sequence
        )

        transform_command = [
            f"--input-file=s3://{timdex_bucket}/{extract_output_file}",
            f"--output-file=s3://{timdex_bucket}/{transform_output_file}",
            f"--source={source}",
        ]

        files_to_transform.append({"transform-command": transform_command})

    return {"files-to-transform": files_to_transform}


def generate_load_commands(
    transform_output_files: list[str], run_type: str, source: str, timdex_bucket: str
) -> dict:
    """Generate task run command for loading records into OpenSearch."""
    if run_type == "daily":
        files_to_index = []
        files_to_delete = []

        for transform_output_file in transform_output_files:
            load_type, _ = helpers.get_load_type_and_sequence_from_timdex_filename(
                transform_output_file
            )
            if load_type == "index":
                load_command = [
                    "bulk-index",
                    "--source",
                    source,
                    f"s3://{timdex_bucket}/{transform_output_file}",
                ]
                files_to_index.append({"load-command": load_command})
            elif load_type == "delete":
                load_command = [
                    "bulk-delete",
                    "--source",
                    source,
                    f"s3://{timdex_bucket}/{transform_output_file}",
                ]
                files_to_delete.append({"load-command": load_command})

        return {"files-to-index": files_to_index, "files-to-delete": files_to_delete}

    if run_type == "full":
        new_index_name = helpers.generate_index_name(source)

        files_to_index = []
        for transform_output_file in transform_output_files:
            load_type, _ = helpers.get_load_type_and_sequence_from_timdex_filename(
                transform_output_file
            )
            if load_type == "delete":
                logger.error(
                    "%s full ingest had a deleted records file: %s",
                    source,
                    transform_output_file,
                )
                continue
            load_command = [
                "bulk-index",
                "--index",
                new_index_name,
                f"s3://{timdex_bucket}/{transform_output_file}",
            ]
            files_to_index.append({"load-command": load_command})

        promote_index_command = ["promote", "--index", new_index_name]
        for alias, sources in config.INDEX_ALIASES.items():
            if source in sources:
                promote_index_command.append("--alias")
                promote_index_command.append(alias)

        return {
            "create-index-command": ["create", "--index", new_index_name],
            "files-to-index": files_to_index,
            "promote-index-command": promote_index_command,
        }

    return {
        "failure": "Something unexpected went wrong. Please check input and try again."
    }
