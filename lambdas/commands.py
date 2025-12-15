import logging
from typing import TYPE_CHECKING

from lambdas import helpers
from lambdas.config import Config

if TYPE_CHECKING:
    from lambdas.format_input import InputPayload

logger = logging.getLogger(__name__)

CONFIG = Config()


def generate_extract_command(input_payload: "InputPayload") -> dict:
    step = "extract"
    source = input_payload.source
    run_type = input_payload.run_type
    run_date = input_payload.run_date
    raw = input_payload.raw
    bucket = CONFIG.timdex_bucket

    output_prefix = helpers.generate_step_output_prefix(input_payload, step)
    output_file = helpers.generate_step_output_filename(
        source, "index", output_prefix, step
    )
    s3_output = f"s3://{bucket}/{output_file}"

    from_date = helpers.generate_harvest_from_date(run_date)

    cmd: list[str] = []
    if input_payload.verbose:
        cmd.append("--verbose")

    if source in CONFIG.GIS_SOURCES:
        cmd.append("harvest")

        if run_type == "daily":
            cmd.extend(["--harvest-type=incremental", f"--from-date={from_date}"])
        elif run_type == "full":
            cmd.append("--harvest-type=full")

        cmd.extend([f"--output-file={s3_output}", source.removeprefix("gis")])

    elif source == "mitlibwebsite":
        cmd.append("harvest")

        cmd.extend(
            [
                f"--config-yaml-file={raw['btrix-config-yaml-file']}",
                f"--records-output-file={s3_output}",
            ]
        )

        if sitemaps := raw.get("btrix-sitemaps"):
            cmd.extend(f"--sitemap={s}" for s in sitemaps)

        if run_type == "daily":
            cmd.append(f"--sitemap-from-date={from_date}")

        if sitemap_urls_out := raw.get("btrix-sitemap-urls-output-file"):
            cmd.append(f"--sitemap-urls-output-file={sitemap_urls_out}")

        if sitemap_urls_previous := raw.get("btrix-previous-sitemap-urls-file"):
            cmd.append(f"--previous-sitemap-urls-file={sitemap_urls_previous}")

    else:
        cmd.extend(
            [
                f"--host={raw['oai-pmh-host']}",
                f"--output-file={s3_output}",
                "harvest",
            ]
        )

        if source in {"aspace", "dspace"}:
            cmd.append("--method=get")

        cmd.append(f"--metadata-format={raw['oai-metadata-format']}")

        if run_type == "daily":
            cmd.append(f"--from-date={from_date}")
        elif run_type == "full":
            cmd.append("--exclude-deleted")

        if set_spec := raw.get("oai-set-spec"):
            cmd.append(f"--set-spec={set_spec}")

    return {"extract-command": cmd}


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
