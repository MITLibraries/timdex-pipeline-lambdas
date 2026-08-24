import pytest
from freezegun import freeze_time

from lambdas import commands
from lambdas.config import Config
from lambdas.format_input import InputPayload


@pytest.fixture(autouse=True)
def _configure_testsource_harvester(monkeypatch):
    source_harvester = {
        harvester_type: [*sources]
        for harvester_type, sources in Config.SOURCE_HARVESTER.items()
    }
    if "testsource" not in source_harvester["oai"]:
        source_harvester["oai"].append("testsource")
    monkeypatch.setattr(Config, "SOURCE_HARVESTER", source_harvester)


def test_generate_extract_command_required_input_fields():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "researchdatabases",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_extract_command(input_payload) == {
        "extract-command": [
            "--host=https://example.com/oai",
            (
                "--output-file=s3://test-timdex-bucket/researchdatabases/"
                "researchdatabases-2022-01-02-daily-extracted-records-to-index.xml"
            ),
            "harvest",
            "--metadata-format=oai_dc",
            "--from-date=2022-01-01",
        ]
    }


def test_generate_extract_command_all_input_fields():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "extract",
        "source": "aspace",
        "verbose": "true",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
        "oai-set-spec": "Collection1",
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_extract_command(input_payload) == {
        "extract-command": [
            "--verbose",
            "--host=https://example.com/oai",
            (
                "--output-file=s3://test-timdex-bucket/aspace/"
                "aspace-2022-01-02-full-extracted-records-to-index.xml"
            ),
            "harvest",
            "--method=get",
            "--metadata-format=oai_dc",
            "--exclude-deleted",
            "--set-spec=Collection1",
        ]
    }


def test_generate_extract_command_geoharvester():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "gismit",
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_extract_command(input_payload) == {
        "extract-command": [
            "harvest",
            "--harvest-type=incremental",
            "--from-date=2022-01-01",
            (
                "--output-file=s3://test-timdex-bucket/gismit/"
                "gismit-2022-01-02-daily-extracted-records-to-index.jsonl"
            ),
            "mit",
        ]
    }


def test_generate_extract_command_browsertrix_full():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "extract",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": [
            "https://libraries.mit.edu/sitemap.xml",
            "https://libraries.mit.edu/news/sitemap.xml",
        ],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
        "btrix-args-json": '{"--hello":"world"}',
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_extract_command(input_payload) == {
        "extract-command": [
            "harvest",
            "--config-yaml-file=s3://bucket/config.yaml",
            (
                "--records-output-file=s3://test-timdex-bucket/mitlibwebsite/"
                "mitlibwebsite-2022-01-02-full-extracted-records-to-index.jsonl"
            ),
            "--sitemap=https://libraries.mit.edu/sitemap.xml",
            "--sitemap=https://libraries.mit.edu/news/sitemap.xml",
            "--sitemap-urls-output-file=s3://bucket/output.txt",
            '--btrix-args-json={"--hello":"world"}',
        ]
    }


def test_generate_extract_command_browsertrix_daily():
    event = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "mitlibwebsite",
        "btrix-config-yaml-file": "s3://bucket/config.yaml",
        "btrix-sitemaps": ["https://libraries.mit.edu/sitemap.xml"],
        "btrix-sitemap-urls-output-file": "s3://bucket/output.txt",
        "btrix-previous-sitemap-urls-file": "s3://bucket/previous.txt",
        "btrix-args-json": '{"--hello":"world"}',
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_extract_command(input_payload) == {
        "extract-command": [
            "harvest",
            "--config-yaml-file=s3://bucket/config.yaml",
            (
                "--records-output-file=s3://test-timdex-bucket/mitlibwebsite/"
                "mitlibwebsite-2022-01-02-daily-extracted-records-to-index.jsonl"
            ),
            "--sitemap=https://libraries.mit.edu/sitemap.xml",
            "--sitemap-from-date=2022-01-01",
            "--sitemap-urls-output-file=s3://bucket/output.txt",
            "--previous-sitemap-urls-file=s3://bucket/previous.txt",
            '--btrix-args-json={"--hello":"world"}',
        ]
    }


def test_generate_transform_commands_required_input_fields(run_id, run_timestamp):
    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "run-id": run_id,
        "run-timestamp": run_timestamp,
    }
    input_payload = InputPayload.from_event(event)
    extract_output_files = [
        "testsource/testsource-2022-01-02-full-extracted-records-to-index.xml"
    ]
    assert commands.generate_transform_commands(
        input_payload,
        extract_output_files,
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    (
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-full-extracted-records-to-index.xml"
                    ),
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                    f"--run-timestamp={run_timestamp}",
                ]
            }
        ]
    }


def test_generate_transform_commands_all_input_fields(run_id, run_timestamp):
    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "source": "testsource",
        "run-id": run_id,
        "run-timestamp": run_timestamp,
    }
    input_payload = InputPayload.from_event(event)
    extract_output_files = [
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_01.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_02.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-delete.xml",
    ]
    assert commands.generate_transform_commands(
        input_payload,
        extract_output_files,
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    (
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-extracted-records-to-index_01.xml"
                    ),
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                    f"--run-timestamp={run_timestamp}",
                ]
            },
            {
                "transform-command": [
                    (
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-extracted-records-to-index_02.xml"
                    ),
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                    f"--run-timestamp={run_timestamp}",
                ]
            },
            {
                "transform-command": [
                    (
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-daily-extracted-records-to-delete.xml"
                    ),
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                    f"--run-timestamp={run_timestamp}",
                ]
            },
        ]
    }


def test_transform_commands_source_with_exclusion_list(
    run_id, run_timestamp, monkeypatch
):
    monkeypatch.setattr(
        Config,
        "SOURCE_EXCLUSION_LISTS",
        {"testsource": "/config/testsource/exclusions.csv"},
    )

    event = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "run-id": run_id,
        "run-timestamp": run_timestamp,
    }
    input_payload = InputPayload.from_event(event)
    extract_output_files = [
        "testsource/testsource-2022-01-02-full-extracted-records-to-index.xml"
    ]
    assert commands.generate_transform_commands(
        input_payload,
        extract_output_files,
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    (
                        "--input-file=s3://test-timdex-bucket/testsource/"
                        "testsource-2022-01-02-full-extracted-records-to-index.xml"
                    ),
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                    f"--run-timestamp={run_timestamp}",
                    "--exclusion-list-path=s3://test-timdex-bucket/config/testsource/exclusions.csv",
                ]
            }
        ]
    }


def test_generate_load_commands_daily(run_id):
    event = {
        "next-step": "load",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "source": "testsource",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_load_commands(input_payload) == {
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            run_id,
            "--source",
            "testsource",
            "s3://test-timdex-bucket/dataset",
        ]
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_no_alias(run_id):
    event = {
        "next-step": "load",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_load_commands(input_payload) == {
        "create-index-command": ["create", "--index", "testsource-2022-01-02t12-13-14"],
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            run_id,
            "--index",
            "testsource-2022-01-02t12-13-14",
            "s3://test-timdex-bucket/dataset",
        ],
        "promote-index-command": ["promote", "--index", "testsource-2022-01-02t12-13-14"],
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_with_alias(run_id):
    event = {
        "next-step": "load",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "alma",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    assert commands.generate_load_commands(input_payload) == {
        "create-index-command": ["create", "--index", "alma-2022-01-02t12-13-14"],
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            run_id,
            "--index",
            "alma-2022-01-02t12-13-14",
            "s3://test-timdex-bucket/dataset",
        ],
        "promote-index-command": [
            "promote",
            "--index",
            "alma-2022-01-02t12-13-14",
            "--alias",
            "timdex",
        ],
    }


def test_generate_load_commands_unhandled_run_type(run_id):
    # test that validation catches invalid run-type before reaching generate_load_commands
    event = {
        "next-step": "load",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "not-supported-run-type",
        "source": "alma",
        "run-id": run_id,
    }
    with pytest.raises(ValueError, match=r"Input 'run-type' value must be one of:"):
        InputPayload.from_event(event)


def test_generate_embeddings_create_command_cpu(run_id):
    """Record count below threshold uses cpu compute env."""
    event = {
        "next-step": "embeddings-create",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    result = commands.generate_embeddings_create_command(input_payload, record_count=100)

    assert result["create"]["job_compute_env"] == "cpu"
    assert "create-embeddings-cpu-" in result["create"]["job_name"]
    assert f"--run-id={run_id}" in result["create"]["command"]


def test_generate_embeddings_create_command_gpu(run_id):
    """Record count at/above threshold uses gpu compute env.

    NOTE: this use of gpu workflow will likely switch back to gpu-spot when
    timdex-embeddings supports retry for gpu-spot instance terminations.
    """
    event = {
        "next-step": "embeddings-create",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    result = commands.generate_embeddings_create_command(input_payload, record_count=500)

    assert result["create"]["job_compute_env"] == "gpu"
    assert "create-embeddings-gpu-" in result["create"]["job_name"]


def test_generate_embeddings_load_command(run_id):
    event = {
        "next-step": "embeddings-load",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "testsource",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    result = commands.generate_embeddings_load_command(input_payload)

    assert result == {
        "load": {
            "bulk-update-embeddings-command": [
                "--verbose",
                "bulk-update-embeddings",
                "--source=testsource",
                f"--run-id={run_id}",
                "s3://test-timdex-bucket/dataset",
            ],
        }
    }


def test_generate_fulltexts_harvest_command(run_id):
    event = {
        "next-step": "fulltexts-harvest",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "dspace",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    result = commands.generate_fulltexts_harvest_command(input_payload)

    assert result == {
        "harvester-type": "dspace-fulltext-harvester",
        "harvest": {"command": ["--verbose", "harvest", f"--run-id={run_id}"]},
    }


def test_generate_fulltexts_load_command(run_id):
    event = {
        "next-step": "fulltexts-load",
        "run-date": "2022-01-02",
        "run-type": "daily",
        "source": "dspace",
        "run-id": run_id,
    }
    input_payload = InputPayload.from_event(event)
    result = commands.generate_fulltexts_load_command(input_payload)

    assert result == {
        "load": {
            "bulk-update-fulltexts-command": [
                "--verbose",
                "bulk-update-fulltexts",
                "--source=dspace",
                f"--run-id={run_id}",
                "s3://test-timdex-bucket/dataset",
            ],
        }
    }
