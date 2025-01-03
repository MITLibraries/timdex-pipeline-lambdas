# ruff: noqa: FBT003

from freezegun import freeze_time

from lambdas import commands


def test_generate_extract_command_required_input_fields():
    input_data = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "testsource",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
    }
    assert commands.generate_extract_command(
        input_data, "2022-01-02", "test-timdex-bucket", False
    ) == {
        "extract-command": [
            "--host=https://example.com/oai",
            "--output-file=s3://test-timdex-bucket/testsource/"
            "testsource-2022-01-02-daily-extracted-records-to-index.xml",
            "harvest",
            "--metadata-format=oai_dc",
            "--from-date=2022-01-01",
        ]
    }


def test_generate_extract_command_all_input_fields():
    input_data = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "next-step": "extract",
        "source": "aspace",
        "verbose": "true",
        "oai-pmh-host": "https://example.com/oai",
        "oai-metadata-format": "oai_dc",
        "oai-set-spec": "Collection1",
    }
    assert commands.generate_extract_command(
        input_data, "2022-01-02", "test-timdex-bucket", True
    ) == {
        "extract-command": [
            "--verbose",
            "--host=https://example.com/oai",
            "--output-file=s3://test-timdex-bucket/aspace/"
            "aspace-2022-01-02-full-extracted-records-to-index.xml",
            "harvest",
            "--method=get",
            "--metadata-format=oai_dc",
            "--exclude-deleted",
            "--set-spec=Collection1",
        ]
    }


def test_generate_extract_command_geoharvester():
    input_data = {
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "next-step": "extract",
        "source": "gismit",
    }
    assert commands.generate_extract_command(
        input_data, "2022-01-02", "test-timdex-bucket", False
    ) == {
        "extract-command": [
            "harvest",
            "--harvest-type=incremental",
            "--from-date=2022-01-01",
            "--output-file=s3://test-timdex-bucket/gismit/"
            "gismit-2022-01-02-daily-extracted-records-to-index.jsonl",
            "mit",
        ]
    }


def test_generate_transform_commands_required_input_fields(etl_version_2, run_id):
    input_data = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "full",
        "source": "testsource",
    }
    extract_output_files = [
        "testsource/testsource-2022-01-02-full-extracted-records-to-index.xml"
    ]
    assert commands.generate_transform_commands(
        extract_output_files,
        input_data,
        "2022-01-02",
        "test-timdex-bucket",
        run_id,
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-full-extracted-records-to-index.xml",
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                ]
            }
        ]
    }


def test_generate_transform_commands_all_input_fields(etl_version_2, run_id):
    input_data = {
        "next-step": "transform",
        "run-date": "2022-01-02T12:13:14Z",
        "run-type": "daily",
        "source": "testsource",
    }
    extract_output_files = [
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_01.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-index_02.xml",
        "testsource/testsource-2022-01-02-daily-extracted-records-to-delete.xml",
    ]
    assert commands.generate_transform_commands(
        extract_output_files, input_data, "2022-01-02", "test-timdex-bucket", run_id
    ) == {
        "files-to-transform": [
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-index_01.xml",
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                ]
            },
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-index_02.xml",
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                ]
            },
            {
                "transform-command": [
                    "--input-file=s3://test-timdex-bucket/testsource/"
                    "testsource-2022-01-02-daily-extracted-records-to-delete.xml",
                    "--output-location=s3://test-timdex-bucket/dataset",
                    "--source=testsource",
                    f"--run-id={run_id}",
                ]
            },
        ]
    }


def test_generate_load_commands_daily(etl_version_2, run_id):
    assert commands.generate_load_commands(
        "testsource",
        "2022-01-02",
        "daily",
        "run-abc-123",
        "test-timdex-bucket",
    ) == {
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            "run-abc-123",
            "--source",
            "testsource",
            "s3://test-timdex-bucket/dataset",
        ]
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_no_alias(etl_version_2, run_id):
    assert commands.generate_load_commands(
        "testsource",
        "2022-01-02",
        "full",
        "run-abc-123",
        "test-timdex-bucket",
    ) == {
        "create-index-command": ["create", "--index", "testsource-2022-01-02t12-13-14"],
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            "run-abc-123",
            "--index",
            "testsource-2022-01-02t12-13-14",
            "s3://test-timdex-bucket/dataset",
        ],
        "promote-index-command": ["promote", "--index", "testsource-2022-01-02t12-13-14"],
    }


@freeze_time("2022-01-02 12:13:14")
def test_generate_load_commands_full_with_alias(etl_version_2, run_id):
    assert commands.generate_load_commands(
        "alma",
        "2022-01-02",
        "full",
        "run-abc-123",
        "test-timdex-bucket",
    ) == {
        "create-index-command": ["create", "--index", "alma-2022-01-02t12-13-14"],
        "bulk-update-command": [
            "bulk-update",
            "--run-date",
            "2022-01-02",
            "--run-id",
            "run-abc-123",
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


def test_generate_load_commands_unhandled_run_type(etl_version_2, run_id):
    assert commands.generate_load_commands(
        "alma",
        "2022-01-02",
        "not-supported-run-type",
        "run-abc-123",
        "test-timdex-bucket",
    ) == {"failure": "Unexpected run-type: 'not-supported-run-type'"}
